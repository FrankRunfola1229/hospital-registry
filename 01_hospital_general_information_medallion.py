"""
01_hospital_general_information_medallion.py

End-to-end (no-secrets) pipeline:
  1) Fetch the *current* CMS Provider Data Catalog download URL for a dataset_id
  2) Download CSV to ADLS Gen2 Bronze (run-date partition)
  3) Bronze (CSV) -> Silver (Delta) -> Gold (Delta aggregates)

Why this exists:
- CMS often rotates the underlying "file" URL for a dataset, so hard-coded CSV links break.
- The stable way is: call the Provider Data Catalog "metastore" API using the dataset_id,
  then read the downloadURL from the JSON response.

Parameters (ADF passes these):
- storage_account: e.g., sthcmedallion01
- run_date:        yyyy-MM-dd (partition)
- dataset_id:      CMS Provider Data Catalog dataset id (default: xubh-q36u)
- download_url_override: optional; if provided, notebook uses it instead of metastore

Assumptions:
- Your Databricks workspace can access ADLS using Managed Identity (recommended:
  Access Connector for Azure Databricks + Unity Catalog external locations/volumes).

Security:
- No keys, no secrets, no PATs.
"""

import json
import re
from datetime import datetime

import requests
from pyspark.sql import functions as F

# ---- widgets / parameters ----
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("dataset_id", "xubh-q36u")
dbutils.widgets.text("download_url_override", "")

storage_account = dbutils.widgets.get("storage_account").strip()
run_date = dbutils.widgets.get("run_date").strip()
dataset_id = dbutils.widgets.get("dataset_id").strip() or "xubh-q36u"
download_url_override = dbutils.widgets.get("download_url_override").strip()

if not storage_account or not run_date:
    raise ValueError("Missing required widgets: storage_account and run_date")

bronze_dir = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
    f"hospital_general_information/run_date={run_date}"
)
bronze_path = f"{bronze_dir}/hospital_general_information.csv"

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/hospital_general_information"
gold_root = f"abfss://gold@{storage_account}.dfs.core.windows.net/hospital_general_information"

print("Dataset ID:", dataset_id)
print("Bronze:", bronze_path)
print("Silver:", silver_path)
print("Gold root:", gold_root)

# ---- helpers ----
def to_snake(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def normalize_nulls(df):
    nullish = ["Not Available", "NOT AVAILABLE", ""]
    for c in df.columns:
        df = df.withColumn(c, F.when(F.col(c).isin(nullish), None).otherwise(F.col(c)))
    return df


def resolve_cms_download_url(dataset_id: str) -> str:
    """Resolve the current download URL for a CMS Provider Data Catalog dataset.

    Tries to read distribution[*].data.downloadURL from the metastore response.
    If it can't find it, raises a ValueError with enough info to debug.
    """

    # NOTE: show-reference-ids=false keeps response cleaner (no internal IDs where possible)
    meta_url = (
        "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/"
        f"{dataset_id}?show-reference-ids=false"
    )

    r = requests.get(meta_url, timeout=60)
    r.raise_for_status()
    meta = r.json()

    dist = meta.get("distribution")

    # distribution can be a dict or list depending on dataset/schema
    candidates = []
    if isinstance(dist, dict):
        candidates = [dist]
    elif isinstance(dist, list):
        candidates = dist

    for d in candidates:
        if not isinstance(d, dict):
            continue
        data = d.get("data") if isinstance(d.get("data"), dict) else {}
        url = (
            data.get("downloadURL")
            or data.get("downloadUrl")
            or d.get("downloadURL")
            or d.get("downloadUrl")
        )
        if url and isinstance(url, str) and url.lower().startswith("http"):
            return url

    # If no downloadURL, surface what we *did* find, because CMS also exposes a SQL API.
    # Many examples use distribution.identifier (a UUID) + /api/1/datastore/sql.
    identifier = None
    if isinstance(dist, dict):
        identifier = dist.get("identifier")
    elif isinstance(dist, list) and dist and isinstance(dist[0], dict):
        identifier = dist[0].get("identifier")

    raise ValueError(
        "Could not find a downloadURL in metastore response. "
        f"dataset_id={dataset_id}. "
        f"Try opening the dataset page and/or inspect the metastore JSON. "
        f"distribution.identifier={identifier}"
    )


def download_csv_to_bronze(url: str, bronze_path: str):
    """Download CSV via HTTPS to a local temp file, then copy to ADLS (abfss)."""

    # Ensure directory exists
    dbutils.fs.mkdirs(bronze_dir)

    tmp_file = f"/tmp/hospital_general_information_{run_date}_{datetime.utcnow().strftime('%H%M%S')}.csv"

    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    # Overwrite bronze file (single file per run date)
    try:
        dbutils.fs.rm(bronze_path, True)
    except Exception:
        pass

    dbutils.fs.cp(f"file:{tmp_file}", bronze_path, True)

    print("✅ Downloaded to Bronze:", bronze_path)


# ---- 1) Resolve the current CSV URL (or use override) ----
if download_url_override:
    download_url = download_url_override
else:
    download_url = resolve_cms_download_url(dataset_id)

print("Resolved download URL:", download_url)

# ---- 2) Land in Bronze ----
download_csv_to_bronze(download_url, bronze_path)

# ---- 3) Bronze read ----
raw = (
    spark.read.option("header", True)
    .option("inferSchema", False)  # portfolio: enforce/convert explicitly below
    .csv(bronze_path)
)

# rename columns
for c in raw.columns:
    raw = raw.withColumnRenamed(c, to_snake(c))

df = normalize_nulls(raw)

# ---- 4) Standardize a few common columns (best-effort) ----
# CMS schema can evolve; if something breaks, print df.columns and update mapping.

# provider_id is common; sometimes it appears as "facility_id".
if "provider_id" not in df.columns:
    if "facility_id" in df.columns:
        df = df.withColumnRenamed("facility_id", "provider_id")
    else:
        raise ValueError(f"Expected 'provider_id' or 'facility_id' not found. Columns: {df.columns}")

# De-dup
_df = df.filter(F.col("provider_id").isNotNull()).dropDuplicates(["provider_id"])

# ZIP cleanup
if "zip_code" in _df.columns:
    _df = _df.withColumn(
        "zip_code",
        F.when(F.col("zip_code").isNull(), None).otherwise(
            F.lpad(F.regexp_extract(F.col("zip_code").cast("string"), r"(\\d+)", 1), 5, "0")
        ),
    )

# Phone digits only
if "phone_number" in _df.columns:
    _df = _df.withColumn("phone_number", F.regexp_replace(F.col("phone_number"), r"[^0-9]", ""))

# Overall rating to int
if "hospital_overall_rating" in _df.columns:
    _df = _df.withColumn("hospital_overall_rating", F.col("hospital_overall_rating").cast("int"))

# Emergency services boolean-ish
if "emergency_services" in _df.columns:
    _df = _df.withColumn(
        "has_emergency_services",
        F.when(F.upper(F.col("emergency_services")) == F.lit("YES"), F.lit(True))
        .when(F.upper(F.col("emergency_services")) == F.lit("NO"), F.lit(False))
        .otherwise(F.lit(None)),
    )

# lineage
_df = _df.withColumn("run_date", F.lit(run_date)).withColumn("ingested_at_utc", F.current_timestamp())

# ---- 5) Write Silver (append) ----
(
    _df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(silver_path)
)

print("✅ Wrote Silver Delta:", silver_path)

# ---- 6) Gold aggregates (overwrite each run for simplicity) ----
silver = spark.read.format("delta").load(silver_path)

# 1) hospitals by state
if "state" in silver.columns:
    by_state = (
        silver.groupBy("state")
        .agg(
            F.countDistinct("provider_id").alias("hospital_count"),
            F.avg("hospital_overall_rating").alias("avg_overall_rating"),
            (F.avg(F.col("has_emergency_services").cast("double")) * 100).alias(
                "pct_with_emergency_services"
            ),
        )
    )
    by_state.write.format("delta").mode("overwrite").save(f"{gold_root}/hospitals_by_state")
    print("✅ Wrote Gold: hospitals_by_state")
else:
    print("⚠️ 'state' column not found; skipping hospitals_by_state")

# 2) rating distribution
if "hospital_overall_rating" in silver.columns:
    rating_dist = (
        silver.groupBy("hospital_overall_rating")
        .agg(F.countDistinct("provider_id").alias("hospital_count"))
        .orderBy("hospital_overall_rating")
    )
    rating_dist.write.format("delta").mode("overwrite").save(f"{gold_root}/rating_distribution")
    print("✅ Wrote Gold: rating_distribution")
else:
    print("⚠️ 'hospital_overall_rating' column not found; skipping rating_distribution")

# 3) hospitals by type
if "hospital_type" in silver.columns:
    by_type = (
        silver.groupBy("hospital_type")
        .agg(
            F.countDistinct("provider_id").alias("hospital_count"),
            F.avg("hospital_overall_rating").alias("avg_overall_rating"),
        )
        .orderBy(F.desc("hospital_count"))
    )
    by_type.write.format("delta").mode("overwrite").save(f"{gold_root}/hospitals_by_type")
    print("✅ Wrote Gold: hospitals_by_type")
else:
    print("⚠️ 'hospital_type' column not found; skipping hospitals_by_type")

# ---- quick validation (optional) ----
print("\n--- Quick validation sample ---")
try:
    (
        spark.read.format("delta")
        .load(f"{gold_root}/hospitals_by_state")
        .orderBy(F.desc("hospital_count"))
        .show(10, False)
    )
except Exception as e:
    print("Validation skipped:", e)
