# Managed Identity notes (no secrets)

You asked for **no secret keys** and to use **managed identity**. Here's the practical checklist.

## A) ADF → ADLS (easy, pure Azure RBAC)
1. Turn on **System assigned managed identity** for the ADF resource.
2. On the Storage account, grant that identity:
   - **Storage Blob Data Contributor** (RBAC)
3. In ADF, create the ADLS linked service using **Managed Identity** auth.

Result: ADF can write Bronze without keys/connection strings.

## B) ADF → Databricks (no PAT token)
Use the Databricks linked service in ADF with **Managed service identity (MSI)** authentication.
This avoids storing a Databricks Personal Access Token (PAT).

High level steps:
- Ensure your Databricks workspace is configured to accept MSI-based access.
- Give ADF MI the needed permissions inside Databricks (workspace / jobs permissions).
- Create ADF Databricks linked service using MSI.

## C) Databricks → ADLS (the part most people do wrong)
If you want *no secrets*, don't use storage keys or service principal client secrets.

Recommended approach:
1. Create **Access Connector for Azure Databricks** (system-assigned MI).
2. Grant the connector RBAC on Storage:
   - **Storage Blob Data Contributor**
3. In Databricks **Unity Catalog**:
   - Create a **Storage Credential** backed by the Access Connector
   - Create **External Locations** for your ADLS paths (silver/gold)
   - Optionally create **Volumes** for friendlier paths

Result: Databricks can read/write ADLS using managed identity (no secret rotation).

---

## Minimal permissions to keep things simple
For portfolio work:
- Storage RBAC: Storage Blob Data Contributor is fine.
- Databricks: least privilege is better, but speed matters for a first project.

---

## If you get stuck
Common blockers:
- Unity Catalog not enabled for the workspace
- External location permission errors (need grants in UC)
- ADF MSI not granted permissions in Databricks

Fix: start by validating each hop:
1) ADF can write Bronze
2) Databricks can read Bronze
3) Databricks can write Silver/Gold
