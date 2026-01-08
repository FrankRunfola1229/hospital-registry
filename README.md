# healthcare-hospitals-medallion01 (ADF + ADLS Gen2 + Databricks, CSV, Managed Identity only)

A small, job-relevant Azure data engineering project using **public healthcare CSV data**, **ADLS Gen2**, **Azure Data Factory**, and **Azure Databricks**.

✅ **No secret keys / no PATs / no connection strings** — use **Managed Identity** end-to-end:
- **ADF** uses its **system-assigned managed identity** to write to ADLS (RBAC).
- **ADF → Databricks** uses **MSI authentication** for the Databricks linked service (no PAT token).
- **Databricks → ADLS** uses a **managed identity** via an **Access Connector for Azure Databricks** + **Unity Catalog external location** (recommended approach for “no secrets”).

---

## 1) Use case (what you say in interviews)

**“Public healthcare provider registry ingestion + quality-ready curated layer.”**

We ingest a public CMS/Medicare hospital registry CSV, land it in a lake (Bronze), clean and standardize it (Silver), and publish analyst-ready aggregates (Gold) like:
- hospitals by state (counts, % with emergency services)
- rating distribution
- hospitals by type

**Data source (public CSV):**
- Hospital General Information (CMS / Medicare Open Data)
  - https://data.medicare.gov/api/views/xubh-q36u/rows.csv?accessType=DOWNLOAD

No PHI. This is safe portfolio data.

---

## 2) Architecture

**Services**
- **ADLS Gen2**: `bronze/`, `silver/`, `gold/`
- **Azure Data Factory**: copy (HTTP→ADLS) + orchestration + run notebook
- **Azure Databricks**: PySpark transform + Delta outputs

**Flow**
1. ADF copies public CSV → ADLS **Bronze** (partitioned by run date)
2. ADF triggers Databricks notebook
3. Databricks reads Bronze CSV → writes **Silver** Delta
4. Databricks creates **Gold** Delta aggregates

```
[Public CMS CSV] --(ADF Copy Activity)--> [ADLS Bronze]
                                   |
                                   +--(ADF Databricks Activity)--> [Databricks Notebook]
                                                                  |-> [ADLS Silver (Delta)]
                                                                  |-> [ADLS Gold (Delta)]
```

---

## 3) Repo structure

```
healthcare-hospitals-medallion01/
├── README.md
├── .gitignore
├── notebooks/
│   └── 01_hospital_general_information_medallion.py
├── adf/
│   └── pipeline_pl_hospital_general_information_to_medallion.md
└── docs/
    └── managed-identity-notes.md
```

---

## 4) Step-by-step (Azure Portal friendly)

### Step 0 — Naming (keep it simple)
- Resource group: `rg-healthcare-hospitals-medallion01`
- Storage account: `sthcmedallion01` (must be globally unique)
- ADF: `adf-healthcare-hospitals01`
- Databricks: `adb-healthcare-hospitals01`

### Step 1 — Create ADLS Gen2 (HNS ON)
1. Create Storage account
2. Advanced → **Enable hierarchical namespace = ON**
3. Create containers:
   - `bronze`
   - `silver`
   - `gold`

Target layout:
- `bronze/hospital_general_information/run_date=YYYY-MM-DD/hospital_general_information.csv`
- `silver/hospital_general_information/` (Delta)
- `gold/hospital_general_information/hospitals_by_state/` (Delta)
- `gold/hospital_general_information/rating_distribution/` (Delta)
- `gold/hospital_general_information/hospitals_by_type/` (Delta)

### Step 2 — Create ADF + Managed Identity (no secrets)
1. Create Data Factory
2. ADF resource → **Identity** → System assigned → **On**
3. Storage account → **Access Control (IAM)** → Add role assignment:
   - Role: **Storage Blob Data Contributor**
   - Assign to: **ADF managed identity**

### Step 3 — Create Databricks + Managed Identity storage access (no secrets)
To do “no secrets” correctly for Databricks reading/writing ADLS:
1. Create **Access Connector for Azure Databricks** (system-assigned MI)
2. Grant that connector RBAC on your storage account:
   - Role: **Storage Blob Data Contributor**
3. In Databricks **Unity Catalog**:
   - Create a **Storage Credential** backed by the Access Connector
   - Create **External Locations** for your `silver` and `gold` paths
   - (Optional) Create **Volumes** for simpler paths

See `docs/managed-identity-notes.md` for a practical checklist.

### Step 4 — ADF pipeline (HTTP → Bronze → Notebook)
Build pipeline: `pl_hospital_general_information_to_medallion`

- Pipeline parameters:
  - `run_date` default: `@formatDateTime(utcNow(),'yyyy-MM-dd')`
  - `source_url` default: `https://data.medicare.gov/api/views/xubh-q36u/rows.csv?accessType=DOWNLOAD`

Activities:
1. **Copy Data** (Source = HTTP dataset, Sink = ADLS Bronze dataset)
2. **Databricks Notebook** activity
   - Notebook: `01_hospital_general_information_medallion`
   - Parameters:
     - `storage_account`
     - `run_date`

The exact build steps are in `adf/pipeline_pl_hospital_general_information_to_medallion.md`.

---

## 5) Run it
1. Trigger the pipeline manually in ADF (or add a daily trigger)
2. Verify Bronze file exists in ADLS
3. Verify Silver/Gold Delta folders are created
4. In Databricks, run the validation cells at the bottom of the notebook

---

## 6) Cost control (do this)
- Use a **single-node** jobs cluster
- **Auto-terminate** at 10–15 minutes
- Don’t leave clusters running
- Run ADF trigger daily or manual

---

## 7) What this proves to employers
- External ingestion with ADF (HTTP → lake)
- Medallion layering (Bronze/Silver/Gold)
- Real PySpark data cleaning + standardization
- Delta Lake outputs
- Managed Identity security patterns (no secrets)

---

## Notes
- Dataset schema can evolve. This notebook is defensive (casts + “Not Available” handling).
- If the CSV column names differ slightly, update the `expected_cols` mapping in the notebook.
