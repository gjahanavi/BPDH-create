from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml


@dataclass
class ValidationError:
    rule: str
    message: str
    column: Optional[str] = None
    row_index: Optional[int] = None
    value: Any = None


def load_rules(path: str) -> Dict[str, Any]:
    """
    Load validation rules from a YAML file.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _enforce_max_indices(indices: List[int], limit: int = 50) -> List[int]:
    """
    Limit the number of row indices we report for readability.
    """
    return indices[:limit]


def validate_excel(xlsx_path: Any, rules_file: str) -> Dict[str, Any]:
    """
    Validate an Excel file against the YAML-driven rules.

    xlsx_path can be a filesystem path, a file-like object, or
    any input accepted by pandas.read_excel.

    Returns a dict:
    {
      "ok": bool,
      "errors": List[ValidationError as dict],
      "df": pandas.DataFrame | None
    }
    """
    rules = load_rules(rules_file)
    errors: List[ValidationError] = []

    try:
        df = pd.read_excel(xlsx_path, engine="openpyxl")
    except Exception as exc:  # pragma: no cover - defensive
        errors.append(
            ValidationError(
                rule="read_error",
                message=f"Failed to read Excel file: {exc}",
            )
        )
        return {"ok": False, "errors": [e.__dict__ for e in errors], "df": None}

    # Normalize columns by stripping whitespace
    df.columns = [str(c).strip() for c in df.columns]

    required_columns = rules.get("required_columns", [])
    optional_columns = rules.get("optional_columns", [])

    # Missing required columns: fail fast
    missing_required = [c for c in required_columns if c not in df.columns]
    if missing_required:
        errors.append(
            ValidationError(
                rule="missing_required_columns",
                message=f"Missing required columns: {', '.join(missing_required)}",
            )
        )
        return {"ok": False, "errors": [e.__dict__ for e in errors], "df": df}

    # Nulls in required columns
    for col in required_columns:
        null_series = df[col].isna()
        failing_indices = _enforce_max_indices(df[null_series].index.tolist())
        for idx in failing_indices:
            errors.append(
                ValidationError(
                    rule="null_required",
                    message=f"Null/blank value in required column '{col}'",
                    column=col,
                    row_index=int(idx),
                    value=None,
                )
            )

    # Email format
    email_rule = rules.get("email", {})
    email_col = email_rule.get("column")
    email_regex = email_rule.get("regex")
    if email_col and email_regex and email_col in df.columns:
        pattern = re.compile(email_regex)
        series = df[email_col].fillna("").astype(str).str.strip()
        invalid_mask = ~series.str.match(pattern) & series.ne("")
        failing_indices = _enforce_max_indices(df[invalid_mask].index.tolist())
        for idx in failing_indices:
            errors.append(
                ValidationError(
                    rule="email_format",
                    message="Invalid email format",
                    column=email_col,
                    row_index=int(idx),
                    value=series.loc[idx],
                )
            )

    # Country in allowed set
    country_rule = rules.get("country", {})
    country_col = country_rule.get("column")
    allowed_countries = set(country_rule.get("allowed_values", []))
    if country_col and country_col in df.columns and allowed_countries:
        series = df[country_col].fillna("").astype(str).str.upper()
        invalid_mask = ~series.isin(allowed_countries)
        failing_indices = _enforce_max_indices(df[invalid_mask].index.tolist())
        for idx in failing_indices:
            errors.append(
                ValidationError(
                    rule="country_allowed_values",
                    message=f"COUNTRY must be one of {sorted(allowed_countries)}",
                    column=country_col,
                    row_index=int(idx),
                    value=series.loc[idx],
                )
            )

    # Unique BP_ID within file
    unique_rule = rules.get("unique", {})
    unique_col = unique_rule.get("column")
    if unique_col and unique_col in df.columns:
        duplicated_mask = df[unique_col].duplicated(keep=False)
        failing_indices = _enforce_max_indices(df[duplicated_mask].index.tolist())
        for idx in failing_indices:
            errors.append(
                ValidationError(
                    rule="unique_bp_id",
                    message="BP_ID must be unique within the file",
                    column=unique_col,
                    row_index=int(idx),
                    value=df.loc[idx, unique_col],
                )
            )

    # Conditional: if BP_TYPE == "VENDOR", COUNTRY must be IN/US/GB
    cond_rule = rules.get("bp_type_country_rule", {})
    bp_type_col = cond_rule.get("bp_type_column")
    country_col_cond = cond_rule.get("country_column")
    bp_type_value = cond_rule.get("bp_type_value")
    allowed_cond_countries = set(cond_rule.get("allowed_countries", []))

    if (
        bp_type_col
        and country_col_cond
        and bp_type_value
        and allowed_cond_countries
        and bp_type_col in df.columns
        and country_col_cond in df.columns
    ):
        bp_series = df[bp_type_col].fillna("").astype(str).str.upper()
        country_series = df[country_col_cond].fillna("").astype(str).str.upper()
        mask_vendor = bp_series.eq(str(bp_type_value).upper())
        mask_invalid_country = ~country_series.isin(allowed_cond_countries)
        failing_mask = mask_vendor & mask_invalid_country
        failing_indices = _enforce_max_indices(df[failing_mask].index.tolist())
        for idx in failing_indices:
            errors.append(
                ValidationError(
                    rule="bp_type_country_condition",
                    message=(
                        f"When {bp_type_col} == '{bp_type_value}', "
                        f"{country_col_cond} must be one of {sorted(allowed_cond_countries)}"
                    ),
                    column=country_col_cond,
                    row_index=int(idx),
                    value=country_series.loc[idx],
                )
            )

    ok = len(errors) == 0
    return {"ok": ok, "errors": [e.__dict__ for e in errors], "df": df}

