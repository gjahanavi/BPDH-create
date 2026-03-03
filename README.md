## BPDH – Business Partner Mass Create

This project is a small Streamlit-based MVP for **BPDH Business Partner Mass Create**.  
It validates uploaded Excel files against YAML-driven rules, generates a **versioned CSV** and a **JSON manifest with SHA-256 hash**, and can optionally upload the CSV to an **SFTP landing zone**.

The app is designed to:

- **Run locally in VS Code** (or any Python environment).
- **Deploy on Streamlit Cloud** with the same UX.

---

### Project structure

- **app.py**: Streamlit UI entry point.
- **src/validation.py**: Excel validation logic driven by YAML rules.
- **src/utils.py**: Utility helpers for filenames, hashing, manifests.
- **src/transfer.py**: SFTP upload + verification using Paramiko.
- **configs/validation_rules.yaml**: Declarative validation rules.
- **sample_input/BPDH_BPCreate_sample.xlsx**: Sample input file.
- **.streamlit/secrets.toml**: Placeholder SFTP configuration (no real secrets).

---

### Validation rules

The rules are defined in `configs/validation_rules.yaml` and currently enforce:

- **Required columns**: `BP_ID`, `BP_NAME`, `COUNTRY`, `BP_TYPE`, `EMAIL`
- **Optional columns**: `PHONE`, `ADDRESS1`, `ADDRESS2`, `CITY`, `ZIP`, `TAX_ID`
- **Email format**: Regex-based validation on `EMAIL`
- **Allowed `COUNTRY` values**: `IN, US, GB, DE, FR, SG, AU, AE`
- **Uniqueness**: `BP_ID` must be unique within a file
- **Conditional rule**: if `BP_TYPE == "VENDOR"`, then `COUNTRY` must be one of `IN, US, GB`

The YAML also exposes a **`schema_version`** that is embedded into the generated manifest.

---

### Running locally

#### 1. Create and activate a virtual environment

**Windows (PowerShell)**:

```powershell
cd bpdh-bp-create
python -m venv .venv
.venv\Scripts\Activate.ps1
```

**macOS / Linux (bash/zsh)**:

```bash
cd bpdh-bp-create
python3 -m venv .venv
source .venv/bin/activate
```

#### 2. Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

#### 3. Run the app

```bash
streamlit run app.py
```

Open the URL shown in the terminal (typically `http://localhost:8501`) in your browser.

---

### Using the UI

1. **Configure sidebar**
   - **ENV**: Select `UAT` or `PROD`.
   - **RITM**: Enter the request / RITM number (e.g., `RITM0123456`).
   - The app version is also displayed.

2. **Step 1 – Upload Excel**
   - Upload an `.xlsx` file that follows the sample template.
   - If no file is uploaded, the app shows guidance instead of failing.

3. **Step 2 – Validate**
   - Click **“Run Validation ✅”**.
   - If there are errors:
     - An **error table** is shown with rule, message, column, row index, and value.
     - At most the **first 50 row indices per rule** are displayed for readability.
     - A **reject CSV** containing problematic rows can be downloaded.
   - If validation passes:
     - A success message is shown.
     - A **preview of the first 100 rows** is displayed.

4. **Step 3 – Generate CSV + manifest**
   - On success, the app:
     - Generates a versioned CSV name:  
       `BPDH_BPCreate_{ENV}_{YYYYMMDD}_{RITM}_{vNN}.csv`
     - Saves it under the `out/` directory.
     - Computes the **SHA-256** hash of the CSV.
     - Creates a JSON manifest containing:
       - `env`, `ritm`, `csv_file`, `sha256`, `schema_version`, `generated_on`
     - Provides **Download buttons** for both the CSV and the manifest.

5. **Step 4 – Optional SFTP upload**
   - Enable the **“Enable SFTP upload (optional)”** checkbox.
   - Fill in:
     - **host**, **port**, **username**
     - **key path** (RSA key on disk)
     - **remote directory**
   - Click **“Upload CSV to SFTP”**:
     - The app uploads the CSV and compares **local vs remote file size**.
     - On mismatch, it raises an error.
     - On success, it shows the remote path.
   - If any parameters are missing, the app **warns gracefully** instead of failing.

---

### SFTP and Streamlit Cloud

- SFTP usually targets on-prem or private servers that **may not be reachable** from **Streamlit Cloud**.
- The SFTP feature is therefore **best used inside a secure internal network** (e.g., running locally or in a VPN-connected environment).
- On Streamlit Cloud, the SFTP step will typically fail unless the target is internet-accessible and firewall rules allow it.

---

### Streamlit Cloud deployment

1. Push this project (including `app.py`, `src/`, `configs/`, `requirements.txt`) to a Git repository (e.g., GitHub).
2. Go to [`https://share.streamlit.io`](https://share.streamlit.io).
3. Sign in and **create a new app**, pointing it at your repository and branch.
4. Set **`app.py`** as the entry point.
5. Configure **secrets**:
   - In Streamlit Cloud, open your app settings → **Secrets**.
   - Add an `[sftp]` section (if you plan to use SFTP):

   ```toml
   [sftp]
   host = "your-sftp-host.example.com"
   port = "22"
   username = "your-username"
   key_path = "/mount/path/to/key"  # if applicable
   remote_dir = "/remote/landing/directory"
   ```

   - Do **not** commit real secrets to Git; use the secrets manager instead.

6. Deploy. Streamlit Cloud will install `requirements.txt` and start the app with the **same UX** as local.

---

### Adjusting validation rules

To change validation behavior, edit `configs/validation_rules.yaml`:

- Add or remove **required** or **optional** columns.
- Adjust the **email regex** (e.g., to tighten or relax allowed patterns).
- Modify allowed **country codes**.
- Change the **uniqueness** key (e.g., different ID column).
- Update the **conditional** rules for `BP_TYPE` and `COUNTRY`.
- Increment `schema_version` when you make breaking changes to the schema.

The app will automatically pick up these changes the next time it runs.

---

### Quick test plan

- **Happy path**
  - Use `sample_input/BPDH_BPCreate_sample.xlsx`.
  - Run validation → should pass.
  - Confirm CSV and manifest are generated and downloadable.
  - Verify manifest `sha256` matches the CSV (e.g., with a local hash tool).

- **Missing required column**
  - Remove `EMAIL` column from a copy of the sample.
  - Upload and validate → should fail fast with a missing-column error.

- **Invalid email**
  - Change one `EMAIL` to `not-an-email`.
  - Validate → should show `email_format` error with row index and value.

- **Invalid country**
  - Set `COUNTRY` to `ZZ` for one row.
  - Validate → `country_allowed_values` error should appear.

- **Duplicate BP_ID**
  - Duplicate a `BP_ID` in two rows.
  - Validate → `unique_bp_id` errors for both rows.

- **Conditional rule**
  - Set a row with `BP_TYPE = "VENDOR"` and `COUNTRY = "FR"`.
  - Validate → should produce `bp_type_country_condition` error.

- **SFTP edge cases**
  - Enable SFTP but leave fields blank → warning about missing parameters.
  - Configure valid SFTP settings (in a suitable environment) and confirm successful upload + size verification.

