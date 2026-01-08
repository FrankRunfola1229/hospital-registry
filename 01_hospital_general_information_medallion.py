"""
01_hospital_general_information_medallion.py

Bronze (CSV) -> Silver (Delta) -> Gold (Delta aggregates)

Parameters (ADF passes these):
- storage_account: e.g., sthcmedallion01
- run_date:        yyyy-MM-dd (used to locate the Bronze landing folder)

Assumptions:
- Bronze landed by ADF at:
  abfss://bronze@<storage>.dfs.core.windows.net/hospital_general_information/run_date=<run_date>/hospital_general_information.csv

Security:
- Use Managed Identity for ADLS access (recommended via Access Connector + Unity Catalog external locations).
"""

import re
from pyspark.sql import functions as F

# ---- widgets / parameters ----
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("run_date", "")

storage_account = dbutils.widgets.get("storage_account").strip()
run_date = dbutils.widgets.get("run_date").strip()

if not storage_account or not run_date:
    raise ValueError("Missing required widgets: storage_account and run_date")

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/hospital_general_information/run_date={run_date}/hospital_general_information.csv"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/hospital_general_information"
gold_root  = f"abfss://gold@{storage_account}.dfs.core.windows.net/hospital_general_information"

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

# ---- bronze read ----
raw = (spark.read
       .option("header", True)
       .option("inferSchema", False)   # portfolio: be explicit later
       .csv(bronze_path))

# rename columns
for c in raw.columns:
    raw = raw.withColumnRenamed(c, to_snake(c))

df = normalize_nulls(raw)

# ---- standardize a few common columns (best-effort) ----
# Provider ID is typically present; if not, the dataset changed—inspect columns and update logic.
if "provider_id" not in df.columns:
    raise ValueError(f"Expected 'provider_id' column not found. Columns: {df.columns}")

df = df.filter(F.col("provider_id").isNotNull()).dropDuplicates(["provider_id"])

# ZIP cleanup
if "zip_code" in df.columns:
    df = df.withColumn(
        "zip_code",
        F.when(F.col("zip_code").isNull(), None)
         .otherwise(F.lpad(F.regexp_extract(F.col("zip_code").cast("string"), r"(\\d+)", 1), 5, "0"))
    )

# Phone digits only
if "phone_number" in df.columns:
    df = df.withColumn("phone_number", F.regexp_replace(F.col("phone_number"), r"[^0-9]", ""))

# Overall rating to int
if "hospital_overall_rating" in df.columns:
    df = df.withColumn("hospital_overall_rating", F.col("hospital_overall_rating").cast("int"))

# Emergency services boolean-ish
if "emergency_services" in df.columns:
    df = df.withColumn(
        "has_emergency_services",
        F.when(F.upper(F.col("emergency_services")) == F.lit("YES"), F.lit(True))
         .when(F.upper(F.col("emergency_services")) == F.lit("NO"), F.lit(False))
         .otherwise(F.lit(None))
    )

# lineage
df = (df
      .withColumn("run_date", F.lit(run_date))
      .withColumn("ingested_at_utc", F.current_timestamp()))

# ---- write silver (append) ----
(df.write
 .format("delta")
 .mode("append")
 .option("mergeSchema", "true")
 .save(silver_path))

print("✅ Wrote Silver Delta:", silver_path)

# ---- gold aggregates (overwrite each run for simplicity) ----
silver = spark.read.format("delta").load(silver_path)

# 1) hospitals by state
if "state" in silver.columns:
    by_state = (silver.groupBy("state")
                .agg(
                    F.countDistinct("provider_id").alias("hospital_count"),
                    F.avg("hospital_overall_rating").alias("avg_overall_rating"),
                    (F.avg(F.col("has_emergency_services").cast("double")) * 100).alias("pct_with_emergency_services")
                ))
    (by_state.write.format("delta").mode("overwrite").save(f"{gold_root}/hospitals_by_state"))
    print("✅ Wrote Gold: hospitals_by_state")
else:
    print("⚠️ 'state' column not found; skipping hospitals_by_state")

# 2) rating distribution
if "hospital_overall_rating" in silver.columns:
    rating_dist = (silver.groupBy("hospital_overall_rating")
                   .agg(F.countDistinct("provider_id").alias("hospital_count"))
                   .orderBy("hospital_overall_rating"))
    (rating_dist.write.format("delta").mode("overwrite").save(f"{gold_root}/rating_distribution"))
    print("✅ Wrote Gold: rating_distribution")
else:
    print("⚠️ 'hospital_overall_rating' column not found; skipping rating_distribution")

# 3) hospitals by type
if "hospital_type" in silver.columns:
    by_type = (silver.groupBy("hospital_type")
               .agg(
                   F.countDistinct("provider_id").alias("hospital_count"),
                   F.avg("hospital_overall_rating").alias("avg_overall_rating")
               )
               .orderBy(F.desc("hospital_count")))
    (by_type.write.format("delta").mode("overwrite").save(f"{gold_root}/hospitals_by_type"))
    print("✅ Wrote Gold: hospitals_by_type")
else:
    print("⚠️ 'hospital_type' column not found; skipping hospitals_by_type")

# ---- quick validation (optional) ----
print("\n--- Quick validation sample ---")
try:
    spark.read.format("delta").load(f"{gold_root}/hospitals_by_state").orderBy(F.desc("hospital_count")).show(10, False)
except Exception as e:
    print("Validation skipped:", e)
