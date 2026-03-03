import io
import json
import os
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from src.transfer import sftp_put_and_verify
from src.utils import (
    render_filename,
    sha256_of_bytes,
    today_str,
    version_tag,
    write_manifest,
)
from src.validation import validate_excel, load_rules


APP_VERSION = "0.1.0"
RULES_PATH = os.path.join("configs", "validation_rules.yaml")
OUT_DIR = "out"


def _build_error_dataframe(errors: List[Dict[str, Any]]) -> pd.DataFrame:
    if not errors:
        return pd.DataFrame()
    df = pd.DataFrame(errors)
    # Make columns easier to read / consistently ordered
    cols = ["rule", "message", "column", "row_index", "value"]
    return df.reindex(columns=[c for c in cols if c in df.columns])


def _collect_reject_rows(errors: List[Dict[str, Any]], df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a dataframe containing all rows that had at least one row-level error.
    """
    row_indices = sorted(
        {
            int(e["row_index"])
            for e in errors
            if e.get("row_index") is not None and 0 <= int(e["row_index"]) < len(df)
        }
    )
    if not row_indices:
        return pd.DataFrame(columns=df.columns)
    return df.iloc[row_indices].copy()


def main() -> None:
    st.set_page_config(
        page_title="BPDH – Business Partner Mass Create",
        layout="wide",
    )

    st.title("BPDH – Business Partner Mass Create")
    st.write(
        "Validate Business Partner Excel files, generate versioned CSVs and manifests, "
        "and optionally upload to SFTP landing. ✅"
    )

    # Sidebar configuration
    st.sidebar.header("Configuration")
    env = st.sidebar.selectbox("Environment (ENV)", ["UAT", "PROD"], index=0)
    ritm = st.sidebar.text_input("RITM / Request ID", value="RITM0000000")
    st.sidebar.markdown(f"**App version:** {APP_VERSION}")

    # Load schema version from YAML
    try:
        rules = load_rules(RULES_PATH)
        schema_version = str(rules.get("schema_version", "1.0"))
    except Exception:
        schema_version = "1.0"

    st.markdown("### 1️⃣ Upload Excel file")
    uploaded = st.file_uploader(
        "Upload a Business Partner Excel file (`.xlsx` only)",
        type=["xlsx"],
    )

    if not uploaded:
        st.info(
            "Please upload an `.xlsx` file following the sample template to begin validation. 📄"
        )
        return

    st.markdown("### 2️⃣ Validate file")
    if st.button("Run Validation ✅"):
        with st.spinner("Validating Excel file..."):
            validation_result = validate_excel(uploaded, RULES_PATH)

        errors = validation_result["errors"]
        df: pd.DataFrame | None = validation_result["df"]

        if not df or df.empty:
            st.error("The uploaded file appears to be empty or unreadable.")
            return

        if errors:
            st.error(
                "Validation failed. Please review the errors below. "
                "Only the first 50 row indices per rule are shown for readability. ❌"
            )

            err_df = _build_error_dataframe(errors)
            st.subheader("Validation error details")
            st.dataframe(err_df, use_container_width=True, height=400)

            reject_df = _collect_reject_rows(errors, df)
            if not reject_df.empty:
                st.subheader("Reject rows (for correction)")
                st.dataframe(reject_df.head(100), use_container_width=True, height=300)

                reject_csv = reject_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download reject rows as CSV",
                    data=reject_csv,
                    file_name="BPDH_BPCreate_reject_rows.csv",
                    mime="text/csv",
                )

            return

        # If we reach here, validation passed
        st.success("Validation succeeded. No issues found. 🎉")

        st.markdown("### 3️⃣ Generate versioned CSV and manifest")

        # For this MVP we always start at version 1 per run
        version_number = 1
        vtag = version_tag(version_number)
        date_str = today_str()
        csv_filename = render_filename(env=env, ritm=ritm, version=version_number, date_str=date_str)

        os.makedirs(OUT_DIR, exist_ok=True)
        local_csv_path = os.path.join(OUT_DIR, csv_filename)

        df.to_csv(local_csv_path, index=False)

        with open(local_csv_path, "rb") as f:
            csv_bytes = f.read()

        sha256 = sha256_of_bytes(csv_bytes)

        st.subheader("Preview of first 100 rows")
        st.dataframe(df.head(100), use_container_width=True, height=400)

        st.subheader("Download artifacts")
        st.download_button(
            label=f"Download CSV ({csv_filename})",
            data=csv_bytes,
            file_name=csv_filename,
            mime="text/csv",
        )

        manifest = {
            "env": env,
            "ritm": ritm,
            "csv_file": csv_filename,
            "sha256": sha256,
            "schema_version": schema_version,
            "generated_on": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }

        manifest_json = json.dumps(manifest, indent=2)
        manifest_filename = csv_filename.replace(".csv", ".manifest.json")
        manifest_path = os.path.join(OUT_DIR, manifest_filename)
        write_manifest(manifest_path, manifest)

        st.download_button(
            label=f"Download manifest ({manifest_filename})",
            data=manifest_json.encode("utf-8"),
            file_name=manifest_filename,
            mime="application/json",
        )

        st.code(manifest_json, language="json")

        st.markdown("### 4️⃣ Optional: Upload to SFTP 🚀")
        sftp_enabled = st.checkbox("Enable SFTP upload (optional)")

        if sftp_enabled:
            sftp_defaults = st.secrets.get("sftp", {}) if hasattr(st, "secrets") else {}

            col1, col2 = st.columns(2)
            with col1:
                host = st.text_input(
                    "SFTP host",
                    value=str(sftp_defaults.get("host", "")),
                )
                port_str = st.text_input(
                    "SFTP port",
                    value=str(sftp_defaults.get("port", "22")),
                )
                username = st.text_input(
                    "SFTP username",
                    value=str(sftp_defaults.get("username", "")),
                )
            with col2:
                key_path = st.text_input(
                    "Path to private key (RSA)",
                    value=str(sftp_defaults.get("key_path", "")),
                )
                remote_dir = st.text_input(
                    "Remote landing directory",
                    value=str(sftp_defaults.get("remote_dir", "/landing")),
                )

            if st.button("Upload CSV to SFTP"):
                missing_fields = [
                    name
                    for name, value in [
                        ("host", host),
                        ("port", port_str),
                        ("username", username),
                        ("key_path", key_path),
                        ("remote_dir", remote_dir),
                    ]
                    if not str(value).strip()
                ]
                if missing_fields:
                    st.warning(
                        "SFTP is enabled, but some parameters are missing: "
                        + ", ".join(missing_fields)
                        + ". Please fill them in before uploading. ⚠️"
                    )
                else:
                    try:
                        port = int(port_str)
                    except ValueError:
                        st.error("SFTP port must be a valid integer.")
                        return

                    with st.spinner("Uploading CSV to SFTP and verifying..."):
                        try:
                            remote_path = sftp_put_and_verify(
                                host=host,
                                port=port,
                                username=username,
                                key_path=key_path,
                                local_path=local_csv_path,
                                remote_dir=remote_dir,
                            )
                        except Exception as exc:  # pragma: no cover - environment-dependent
                            st.error(f"SFTP upload failed: {exc}")
                        else:
                            st.success(
                                f"SFTP upload completed successfully. "
                                f"Remote path: `{remote_path}` ✅"
                            )


if __name__ == "__main__":
    main()

