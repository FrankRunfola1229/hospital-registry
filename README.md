# hospital-registry (ADF + ADLS Gen2 + Databricks, CSV, Managed Identity only)

A small, job-relevant Azure data engineering project using **public healthcare CSV data**, **ADLS Gen2**, **Azure Data Factory**, and **Azure Databricks**.

✅ **No secret keys / no PATs / no connection strings** — use **Managed Identity** end-to-end:
- **ADF** uses its **system-assigned managed identity** to orchestrate and (optionally) write to ADLS (RBAC).
- **ADF → Databricks** uses **MSI authentication** for the Databricks linked service (no PAT token).
- **Databricks → ADLS** uses a **managed identity** via an **Access Connector for Azure Databricks** + **Unity Catalog external location** (recommended approach for “no secrets”).

---

## 1) Use case 

**“Public healthcare provider registry ingestion + quality-ready curated layer.”**

1. Ingest public CMS hospital registry dataset, land it in a lake (Bronze)
2. Clean and standardize it (Silver)
3. Publish analyst-ready aggregates (Gold)
   - hospitals by state (counts, % with emergency services)
   - rating distribution
   - hospitals by type

No PHI. This is safe portfolio data.

---

## 2) Why your CSV link broke (and how this project avoids it)

The CMS Provider Data Catalog often rotates the underlying **download file URL** behind a dataset. That’s why links like “`.../Hospital_General_Info.csv

`” or older Data.Medicare.gov export links can start failing.

**Fix:** don’t hard-code the CSV file URL. Resolve the **current** download URL at runtime using the Provider Data Catalog **metastore API** and the dataset’s stable id:
- Dataset page (stable id): `xubh-q36u` (Hospital General Information)
- Metastore API pattern:
  - `https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/<DATASET_ID>?show-reference-ids=false`
  - `https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/xubh-q36u?show-reference-ids=false`

This is the approach used in the included Databricks notebook.

---

## 3) Architecture

**Services**
- **ADLS Gen2**: `bronze/`, `silver/`, `gold/`
- **Azure Data Factory**: orchestration + run notebook (optionally: Web+Copy)
- **Azure Databricks**: download + PySpark transforms + Delta outputs

**Flow (default, most reliable)**
1. ADF triggers Databricks notebook
2. Notebook resolves the current CMS download URL (metastore API)
3. Notebook downloads CSV → ADLS **Bronze** (partitioned by run date)
4. Notebook writes **Silver** Delta + **Gold** aggregates

```
[CMS Provider Data Catalog] --(metastore API)-> [Databricks]
                                       |-> download CSV -> [ADLS Bronze]
                                       |-> transform     -> [ADLS Silver (Delta)]
                                       |-> aggregate     -> [ADLS Gold (Delta)]
            ADF (MSI)  -----------------+-> orchestrate notebook run
```

---

## 4) Repo structure

```
hospital-registry/
├── README.md
├── .gitignore
├── notebooks/
│   └── 01_hospital_general_info_medallion.py


├── adf/
│   └── Hospital_Gen_Info.md


└── docs/
    └── managed-identity-notes.md
```

---

## 5) Step-by-step (Azure Portal friendly)

### Step 0 — Naming (keep it simple)
- Resource group: `rg-hospital-registry`
- Storage account: `storehospitalregistry` (must be globally unique)
- ADF: `adf-hospital-registry`
- Databricks: `adb-hospital-registry`

### Step 1 — Create ADLS Gen2 (HNS ON)
1. Create Storage account
2. Advanced → **Enable hierarchical namespace = ON**
3. Create containers:
   - `bronze`
   - `silver`
   - `gold`

Target layout:
- `bronze/hospital_general_info/run_date=YYYY-MM-DD/Hospital_General_Info.csv`
- `silver/hospital_general_info/` (Delta)
- `gold/hospital_general_info/hospitals_by_state/` (Delta)
- `gold/hospital_general_info/rating_distribution/` (Delta)
- `gold/hospital_general_info/hospitals_by_type/` (Delta)

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
   - Create **External Locations** for your `bronze/`, `silver/`, and `gold/` paths

See `docs/managed-identity-notes.md` for a practical checklist.

### Step 4 — Import the notebook
Upload `notebooks/01_hospital_general_info_medallion.py

` to your Databricks workspace.

### Step 5 — Build the ADF pipeline (orchestrate notebook)
Follow `adf/Hospital_Gen_Info.md`.

Default parameters:
- `dataset_id = xubh-q36u`
- `run_date = today`

---

## 6) Run it
1. Trigger the pipeline manually in ADF
2. Verify Bronze file exists in ADLS
3. Verify Silver/Gold Delta folders are created
4. In Databricks, check the printed “Quick validation sample” output

---

## 7) Cost control (do this)
- Use a **single-node** jobs cluster
- **Auto-terminate** at 10–15 minutes
- Don’t leave clusters running
- Run ADF trigger daily or manual

---

## 8) What this proves to employers
- Orchestration with ADF + parameters
- Medallion layering (Bronze/Silver/Gold)
- Real PySpark cleaning + standardization + aggregation
- Delta Lake outputs
- Managed Identity security patterns (no secrets)

---

## Notes
- CMS dataset schema can evolve. This notebook is defensive (casts + “Not Available” handling).
- If the provider id column name changes, inspect columns and update the mapping section.
