# ADF Pipeline: pl_hospital_general_information_to_medallion

This doc tells you exactly what to build in **Azure Data Factory**.

## Why your CSV links broke
The CMS Provider Data Catalog frequently rotates the underlying download file URL. If you hard-code a CSV URL, it *will* eventually 404.

This pipeline avoids that by having Databricks resolve the current download URL at runtime via the Provider Data Catalog **metastore** API.

---

## Goal
- Orchestrate a Databricks notebook that:
  1) resolves the current CMS CSV download URL (no keys)
  2) downloads to ADLS **Bronze**
  3) writes **Silver/Gold** Delta outputs

You still get the real ADF skill employers care about: linked services, parameters, notebook orchestration, and MI-based security.

---

## 1) Linked Services

### 1.1 ADLS Gen2
- Type: Azure Data Lake Storage Gen2
- Authentication: **Managed Identity**
- Storage account: your ADLS account

**RBAC required**
- Storage account IAM:
  - Role: **Storage Blob Data Contributor**
  - Assign to: ADF **system-assigned managed identity**

### 1.2 Azure Databricks
- Type: Azure Databricks
- Authentication: **Managed service identity (MSI)** (no PAT token)
- Workspace: select your Databricks workspace

---

## 2) Pipeline

Pipeline name:
- `pl_hospital_general_information_to_medallion`

### 2.1 Parameters
- `run_date` (String)
  - Default: `@formatDateTime(utcNow(),'yyyy-MM-dd')`
- `storage_account` (String)
  - Default: your storage account name (e.g., `sthcmedallion01`)
- `dataset_id` (String)
  - Default: `xubh-q36u` (Hospital General Information)

### 2.2 Activities

#### Activity 1 — Databricks Notebook
- Linked service: Databricks (MSI)
- Notebook path:
  - `/Workspace/.../notebooks/01_hospital_general_information_medallion`
- Base parameters:
  - `storage_account`: `@{pipeline().parameters.storage_account}`
  - `run_date`: `@{pipeline().parameters.run_date}`
  - `dataset_id`: `@{pipeline().parameters.dataset_id}`

---

## 3) Trigger
Start manual first. After it works, add a daily trigger.

---

## 4) Verification checklist
- ADLS Bronze has a new file at:
  - `bronze/hospital_general_information/run_date=YYYY-MM-DD/hospital_general_information.csv`
- ADLS Silver has Delta output at:
  - `silver/hospital_general_information/`
- ADLS Gold has Delta outputs at:
  - `gold/hospital_general_information/hospitals_by_state/`
  - `gold/hospital_general_information/rating_distribution/`
  - `gold/hospital_general_information/hospitals_by_type/`

---

## Optional: if you want ADF to do the HTTP→Bronze copy anyway
If you want to show off a more “classic” ingestion pattern in ADF:

1) Add a **Web** activity `GetDatasetMetadata`
- URL:
  - `https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/@{pipeline().parameters.dataset_id}?show-reference-ids=false`

2) Add a **Set variable** activity `SetDownloadUrl`
- Variable: `download_url`
- Value (adjust if the JSON shape differs):
  - `@activity('GetDatasetMetadata').output.distribution[0].data.downloadURL`

3) Add a **Copy Data** activity
- Source: HTTP (Anonymous)
- URL: `@variables('download_url')`
- Sink: ADLS Bronze (same folder layout)

Then keep the Databricks notebook activity (but pass `download_url_override` so the notebook skips resolving again).

Reason I made this optional: the metastore response shape can vary, and I don’t want your first run blocked on ADF JSON-path quirks.
