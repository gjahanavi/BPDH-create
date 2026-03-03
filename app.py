import json
import os
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from config.settings import get_env_config
from modules.batch_executor import run_step6_batch
from modules.dih_executor import (
    run_step4_pre_landing,
    run_step5_landing,
    run_step7_downstream,
)
from modules.logging_utils import get_logger
from modules.reports import generate_reject_report, generate_success_report
from modules.servicenow_client import attach_files, update_ticket
from modules.sftp_handler import upload_with_retry
from src.utils import (
    render_filename,
    sha256_of_bytes,
    today_str,
    version_tag,
    write_manifest,
)
from src.validation import validate_excel, load_rules


APP_VERSION = "0.2.0"
RULES_PATH = os.path.join("configs", "validation_rules.yaml")
OUT_DIR = "out"
logger = get_logger()


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
        "Validate Business Partner Excel files and generate versioned CSVs and manifests. ✅"
    )

    # Sidebar configuration
    st.sidebar.header("Configuration")
    env = st.sidebar.selectbox("Environment (ENV)", ["DEV", "UAT", "PROD"], index=1)
    ritm = st.sidebar.text_input("RITM / SR / Incident", value="RITM0000000")
    snow_sys_id = st.sidebar.text_input("ServiceNow sys_id (optional)", value="")
    batch_id = st.sidebar.text_input("Batch ID", value="BATCH001")
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

        if df is None or df.empty:
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

        # =========================
        # Automated pipeline: Steps 3–7
        # =========================
        st.markdown("### 4️⃣–7️⃣ Automated BPDH Pipeline")

        env_cfg = get_env_config(env)

        if "pipeline_abort" not in st.session_state:
            st.session_state["pipeline_abort"] = False

        def abort_check() -> bool:
            return bool(st.session_state.get("pipeline_abort", False))

        start_col, abort_col = st.columns(2)
        with start_col:
            start_pipeline = st.button("Start Pipeline")
        with abort_col:
            if st.button("Abort Pipeline"):
                st.session_state["pipeline_abort"] = True
                st.warning("Abort requested. Current and subsequent steps will halt as soon as possible.")

        log_expander = st.expander("View Execution Logs", expanded=True)
        log_area = log_expander.empty()
        progress = st.progress(0)

        status = {
            "step3": "PENDING",
            "step4": "PENDING",
            "step5": "PENDING",
            "step6": "PENDING",
            "step7": "PENDING",
        }
        STATUS_ICONS = {
            "PENDING": "⏳",
            "RUNNING": "🔄",
            "DONE": "✅",
            "FAILED": "❌",
        }

        def render_status() -> None:
            cols = st.columns(5)
            labels = [
                "3: SFTP Upload",
                "4: Pre-Landing",
                "5: Landing",
                "6: Batch Jobs",
                "7: Downstream",
            ]
            keys = ["step3", "step4", "step5", "step6", "step7"]
            for col, key, label in zip(cols, keys, labels):
                with col:
                    st.markdown(f"**{label}**")
                    icon = STATUS_ICONS[status[key]]
                    st.markdown(f"{icon} {status[key]}")

        st.markdown("#### Pipeline Status")
        render_status()

        st.markdown("#### Record Count Summary")
        summary_placeholder = st.empty()

        record_summary: Dict[str, int] = {}
        timestamps: Dict[str, str] = {}
        success_report_path = ""
        reject_report_path = ""

        def append_log(line: str) -> None:
            existing = st.session_state.get("log_buffer", "")
            new_text = (existing + "\n" + line).strip()
            st.session_state["log_buffer"] = new_text
            log_area.text(new_text)

        if start_pipeline:
            st.session_state["pipeline_abort"] = False
            append_log("Starting automated pipeline...")
            logger.info("Pipeline started for batch %s", batch_id)

            try:
                # STEP 3 — SFTP upload
                status["step3"] = "RUNNING"
                render_status()
                progress.progress(10)

                ok3, remote_path = upload_with_retry(
                    env_cfg,
                    local_csv_path=local_csv_path,
                    abort_check=abort_check,
                )
                if not ok3:
                    status["step3"] = "FAILED"
                    render_status()
                    if snow_sys_id:
                        update_ticket(
                            env_cfg,
                            snow_sys_id,
                            "failure",
                            "Step 3 (SFTP upload) failed",
                            "Pipeline stopped at Step 3",
                        )
                    st.error("Step 3 failed. See logs for details.")
                    return

                status["step3"] = "DONE"
                timestamps["step3"] = datetime.utcnow().isoformat()
                render_status()
                progress.progress(25)

                # STEP 4 — Pre-landing
                status["step4"] = "RUNNING"
                render_status()
                ok4, pre_count = run_step4_pre_landing(
                    env_cfg,
                    batch_id=batch_id,
                    csv_filename=csv_filename,
                    abort_check=abort_check,
                )
                record_summary["pre_landing"] = pre_count
                if not ok4:
                    status["step4"] = "FAILED"
                    render_status()
                    if snow_sys_id:
                        update_ticket(
                            env_cfg,
                            snow_sys_id,
                            "failure",
                            "Step 4 (pre-landing) failed",
                            "Pipeline stopped at Step 4",
                        )
                    st.error("Step 4 failed. See logs for details.")
                    return

                status["step4"] = "DONE"
                timestamps["step4"] = datetime.utcnow().isoformat()
                render_status()
                progress.progress(40)

                # STEP 5 — Landing
                status["step5"] = "RUNNING"
                render_status()
                ok5, rows = run_step5_landing(
                    env_cfg,
                    batch_id=batch_id,
                    abort_check=abort_check,
                )
                if not ok5:
                    status["step5"] = "FAILED"
                    render_status()
                    if snow_sys_id:
                        update_ticket(
                            env_cfg,
                            snow_sys_id,
                            "failure",
                            "Step 5 (landing load) failed",
                            "Pipeline stopped at Step 5",
                        )
                    st.error("Step 5 failed. See logs for details.")
                    return

                valid_count = 0
                invalid_count = 0
                for cnt, status_str in rows:
                    if str(status_str).upper() == "VALID":
                        valid_count = int(cnt)
                    else:
                        invalid_count += int(cnt)

                record_summary["landing_valid"] = valid_count
                record_summary["landing_invalid"] = invalid_count

                if valid_count == 0:
                    status["step5"] = "FAILED"
                    render_status()
                    if snow_sys_id:
                        update_ticket(
                            env_cfg,
                            snow_sys_id,
                            "failure",
                            "Zero VALID records in landing",
                            "Pipeline stopped at Step 5",
                        )
                    st.error("No valid records found in landing. Halting pipeline.")
                    return

                status["step5"] = "DONE"
                timestamps["step5"] = datetime.utcnow().isoformat()
                render_status()
                progress.progress(60)

                # STEP 6 — Batch jobs
                status["step6"] = "RUNNING"
                render_status()
                ok6, staging_count, bo_count = run_step6_batch(
                    env_cfg,
                    batch_id=batch_id,
                    abort_check=abort_check,
                )
                record_summary["staging"] = staging_count
                record_summary["bo"] = bo_count
                if not ok6:
                    status["step6"] = "FAILED"
                    render_status()
                    if snow_sys_id:
                        update_ticket(
                            env_cfg,
                            snow_sys_id,
                            "failure",
                            "Step 6 (batch jobs) failed",
                            "Pipeline stopped at Step 6",
                        )
                    st.error("Step 6 failed. See logs for details.")
                    return

                status["step6"] = "DONE"
                timestamps["step6"] = datetime.utcnow().isoformat()
                render_status()
                progress.progress(80)

                # STEP 7 — Downstream
                status["step7"] = "RUNNING"
                render_status()
                ok7, downstream_cnt = run_step7_downstream(
                    env_cfg,
                    batch_id=batch_id,
                    log_callback=append_log,
                    abort_check=abort_check,
                )
                record_summary["downstream_success"] = downstream_cnt
                if not ok7:
                    status["step7"] = "FAILED"
                    render_status()
                    if snow_sys_id:
                        update_ticket(
                            env_cfg,
                            snow_sys_id,
                            "failure",
                            "Step 7 (downstream) failed",
                            "Pipeline stopped at Step 7",
                        )
                    st.error("Step 7 failed. See logs for details.")
                    return

                status["step7"] = "DONE"
                timestamps["step7"] = datetime.utcnow().isoformat()
                render_status()
                progress.progress(100)

                # Reports
                success_report_path = generate_success_report(
                    out_dir=OUT_DIR,
                    batch_id=batch_id,
                    filename=csv_filename,
                    env=env,
                    sr_number=ritm,
                    total_records=len(df),
                    loaded_records=record_summary.get("downstream_success", 0),
                    step_timestamps=timestamps,
                )
                # NOTE: reject details would typically come from DB2 error tables.
                reject_report_path = generate_reject_report(
                    out_dir=OUT_DIR,
                    batch_id=batch_id,
                    rejects=[],
                )

                if snow_sys_id:
                    update_ticket(
                        env_cfg,
                        snow_sys_id,
                        "success",
                        "BPDH pipeline completed successfully",
                        "Pipeline completed successfully",
                    )
                    attach_files(
                        env_cfg,
                        snow_sys_id,
                        [success_report_path, reject_report_path],
                    )

                st.success("Pipeline completed successfully ✅")

            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Pipeline failed with exception")
                st.error(f"Pipeline failed: {exc}")
                if snow_sys_id:
                    update_ticket(
                        env_cfg,
                        snow_sys_id,
                        "failure",
                        f"Exception: {exc}",
                        "Pipeline failed unexpectedly",
                    )

        if record_summary:
            df_summary = pd.DataFrame(
                [{"Stage": k, "Count": v} for k, v in record_summary.items()]
            )
            summary_placeholder.dataframe(df_summary, use_container_width=True)

        if success_report_path:
            with open(success_report_path, "rb") as f:
                st.download_button(
                    "Download Success Report",
                    f.read(),
                    file_name=os.path.basename(success_report_path),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
        if reject_report_path:
            with open(reject_report_path, "rb") as f:
                st.download_button(
                    "Download Reject Report",
                    f.read(),
                    file_name=os.path.basename(reject_report_path),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )


if __name__ == "__main__":
    main()

