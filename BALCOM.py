from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.sql.functions import col, split, trim, concat, regexp_replace
from common.createSpark import create_spark_session
from common.read_write_oracle import read_oracle

# 1. Initialize Spark Session
spark = create_spark_session("Report", "hdfs://10.0.17.76:8020")

# 2. Read and Parse HDFS Report
report = spark.read.format("text").load("hdfs://10.0.17.76:8020/reports/ba*")
report = report.filter(col("value").rlike("^[0-9]"))
report = report.withColumn("temp", split(trim(col("value")), "\s+")) \
    .select(
        col("temp").getItem(0).alias("GLCC"),
        col("temp").getItem(1).alias("BANCS"),
        col("temp").getItem(2).alias("GLIF"),
        col("temp").getItem(3).alias("DIFFERENCE"),
        col("temp").getItem(4).alias("DIFF_BW_YESTERDAY") 
    )

# 3. Read Oracle Data
difference_sql = f"(select * from DIFFERENCE) t"
oracle_db = read_oracle(spark, difference_sql)
oracle_db = oracle_db.select(
    concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")).alias("GLCC"),
    col("CBS_BALANCE").alias("BANCS"),
    col("GL_BALANCE").alias("GLIF"),
    col("DIFFERENCE_AMOUNT").alias("DIFFERENCE"),
    col("DIFF_BW_YESTERDAY")
)

# 4. Clean Report Data
report = report \
    .withColumn("BANCS", F.regexp_replace("BANCS", "[,-]", "").cast("double")) \
    .withColumn("GLIF", F.regexp_replace("GLIF", "[,-]", "").cast("double")) \
    .withColumn("DIFFERENCE", F.regexp_replace("DIFFERENCE", "[,-]", "").cast("double")) \
    .withColumn("DIFF_BW_YESTERDAY", F.regexp_replace("DIFF_BW_YESTERDAY", ",", "").cast("double"))

# Cast Oracle numeric columns to double for consistent comparison
oracle_db = oracle_db \
    .withColumn("BANCS", col("BANCS").cast("double")) \
    .withColumn("GLIF", col("GLIF").cast("double")) \
    .withColumn("DIFFERENCE", col("DIFFERENCE").cast("double")) \
    .withColumn("DIFF_BW_YESTERDAY", col("DIFF_BW_YESTERDAY").cast("double"))

# ─────────────────────────────────────────────
# 5. COMPARISON DATAFRAMES
# ─────────────────────────────────────────────

# Alias both sides for unambiguous column references
rpt = report.alias("rpt")
orc = oracle_db.alias("orc")

# Inner join on GLCC (present in both)
joined = rpt.join(orc, on="GLCC", how="inner")

# ── DF1: GLCC present in both AND all 4 columns match ──────────────────────
df_matched = joined.filter(
    (F.coalesce(col("rpt.BANCS"),          F.lit(0)) == F.coalesce(col("orc.BANCS"),          F.lit(0))) &
    (F.coalesce(col("rpt.GLIF"),           F.lit(0)) == F.coalesce(col("orc.GLIF"),           F.lit(0))) &
    (F.coalesce(col("rpt.DIFFERENCE"),     F.lit(0)) == F.coalesce(col("orc.DIFFERENCE"),     F.lit(0))) &
    (F.coalesce(col("rpt.DIFF_BW_YESTERDAY"), F.lit(0)) == F.coalesce(col("orc.DIFF_BW_YESTERDAY"), F.lit(0)))
).select(
    col("GLCC"),
    col("rpt.BANCS").alias("BANCS_REPORT"),
    col("orc.BANCS").alias("BANCS_ORACLE"),
    col("rpt.GLIF").alias("GLIF_REPORT"),
    col("orc.GLIF").alias("GLIF_ORACLE"),
    col("rpt.DIFFERENCE").alias("DIFFERENCE_REPORT"),
    col("orc.DIFFERENCE").alias("DIFFERENCE_ORACLE"),
    col("rpt.DIFF_BW_YESTERDAY").alias("DIFF_BW_YESTERDAY_REPORT"),
    col("orc.DIFF_BW_YESTERDAY").alias("DIFF_BW_YESTERDAY_ORACLE"),
)

# ── DF2: GLCC present in both BUT at least one column differs ───────────────
df_mismatched = joined.filter(
    (F.coalesce(col("rpt.BANCS"),          F.lit(0)) != F.coalesce(col("orc.BANCS"),          F.lit(0))) |
    (F.coalesce(col("rpt.GLIF"),           F.lit(0)) != F.coalesce(col("orc.GLIF"),           F.lit(0))) |
    (F.coalesce(col("rpt.DIFFERENCE"),     F.lit(0)) != F.coalesce(col("orc.DIFFERENCE"),     F.lit(0))) |
    (F.coalesce(col("rpt.DIFF_BW_YESTERDAY"), F.lit(0)) != F.coalesce(col("orc.DIFF_BW_YESTERDAY"), F.lit(0)))
).select(
    col("GLCC"),
    col("rpt.BANCS").alias("BANCS_REPORT"),
    col("orc.BANCS").alias("BANCS_ORACLE"),
    (F.coalesce(col("rpt.BANCS"), F.lit(0)) - F.coalesce(col("orc.BANCS"), F.lit(0))).alias("BANCS_DIFF"),

    col("rpt.GLIF").alias("GLIF_REPORT"),
    col("orc.GLIF").alias("GLIF_ORACLE"),
    (F.coalesce(col("rpt.GLIF"), F.lit(0)) - F.coalesce(col("orc.GLIF"), F.lit(0))).alias("GLIF_DIFF"),

    col("rpt.DIFFERENCE").alias("DIFFERENCE_REPORT"),
    col("orc.DIFFERENCE").alias("DIFFERENCE_ORACLE"),
    (F.coalesce(col("rpt.DIFFERENCE"), F.lit(0)) - F.coalesce(col("orc.DIFFERENCE"), F.lit(0))).alias("DIFFERENCE_DIFF"),

    col("rpt.DIFF_BW_YESTERDAY").alias("DIFF_BW_YESTERDAY_REPORT"),
    col("orc.DIFF_BW_YESTERDAY").alias("DIFF_BW_YESTERDAY_ORACLE"),
)

# ── DF3: GLCC missing in Oracle (present in report only) ────────────────────
df_missing_in_oracle = rpt.join(orc, on="GLCC", how="left_anti") \
    .select(
        col("GLCC"),
        col("BANCS"),
        col("GLIF"),
        col("DIFFERENCE"),
        col("DIFF_BW_YESTERDAY"),
    )

# ── DF4: GLCC missing in Report (present in oracle only) ────────────────────
df_missing_in_report = orc.join(rpt, on="GLCC", how="left_anti") \
    .select(
        col("GLCC"),
        col("BANCS"),
        col("GLIF"),
        col("DIFFERENCE"),
        col("DIFF_BW_YESTERDAY"),
    )

# ─────────────────────────────────────────────
# 6. EXPORT ALL 4 TO HDFS CSV
# ─────────────────────────────────────────────

BASE = "hdfs://10.0.17.76:8020/reports/comparison"

df_matched.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv(f"{BASE}/matched")

df_mismatched.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv(f"{BASE}/mismatched")

df_missing_in_oracle.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv(f"{BASE}/missing_in_oracle")

df_missing_in_report.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv(f"{BASE}/missing_in_report")

print("Comparison exports complete.")
print(f"  Matched rows       : {df_matched.count()}")
print(f"  Mismatched rows    : {df_mismatched.count()}")
print(f"  Missing in Oracle  : {df_missing_in_oracle.count()}")
print(f"  Missing in Report  : {df_missing_in_report.count()}")

# ─────────────────────────────────────────────
# 7. EXPORT ALL 4 TO SINGLE TEXT FILE
# ─────────────────────────────────────────────

def df_to_show_string(df, label, max_rows=10000):
    count = df.count()
    header = f"\n{'='*80}\n  {label}  (Total Rows: {count})\n{'='*80}\n"
    if count == 0:
        return header + "  No records found.\n"
    table = df._jdf.showString(count, 50, False)
    return header + table

sections = [
    (df_matched,           "1. MATCHED  —  GLCC present in both, all columns agree"),
    (df_mismatched,        "2. MISMATCHED  —  GLCC present in both, values differ (with deltas)"),
    (df_missing_in_oracle, "3. MISSING IN ORACLE  —  GLCC exists in Report but not in Oracle"),
    (df_missing_in_report, "4. MISSING IN REPORT  —  GLCC exists in Oracle but not in Report"),
]

full_report = "ORACLE vs REPORT — RECONCILIATION SUMMARY\n" + "="*80 + "\n"

for df, label in sections:
    full_report += df_to_show_string(df, label)

# Write locally then move to HDFS if needed
local_path = "/tmp/reconciliation_report.txt"
with open(local_path, "w") as f:
    f.write(full_report)

print(f"Text report written to {local_path}")

# Optional: push the single file to HDFS
import subprocess
subprocess.run([
    "hdfs", "dfs", "-copyFromLocal", "-f",
    local_path,
    "hdfs://10.0.17.76:8020/reports/comparison/reconciliation_report.txt"
], check=True)
print("Uploaded to HDFS: /reports/comparison/reconciliation_report.txt")





































































































from oracle im getting a dataframe with columns
+------------------+--------+------+----------+-----------------+
|glcc              |BANCS   |GLIF  |DIFFERENCE|DIFF_BW_YESTERDAY|
+------------------+--------+------+----------+-----------------+
|64392INR1017860443|240.0000|0.0000|-240.0000 |-240.0000        |
+------------------+--------+------+----------+-----------------+

from report i have the columns
+------------------+------------+------------+-----------------+-----------------+
|GLCC              |BANCS       |GLIF        |DIFFERENCE       |DIFF_BW_YESTERDAY|
+------------------+------------+------------+-----------------+-----------------+
|04298INR1204505001|0           |2608090     |A/C              |MISING           |
|04292INR1158505004|2058804760  |2058859360  |54600.0000       |ZERO             |
|04430INR1144505005|79413126680 |312474705621|233061578941.3000|5336349479.2100  |

now my task is to compare the both dataframes
from comparision i have to create new dataframes
1)the glcc which present in both dataframes and matches BANCS,GLIF,DIFFERENCE,DIFF_BW_YESTERDAY
2)the glcc which present in both dataframes and doesn't matchesBANCS,GLIF,DIFFERENCE,DIFF_BW_YESTERDAY and also calculate the difference between oracle and report as BANCS(report-oracle) GLIF(report-oracle) DIFF(report-oracle)

3)the glcc that are missing in oracle
4)the glcc that are missing in reportfrom pyspark.sql import SparkSession 

from pyspark.sql import functions as F
from pyspark.sql.functions import col, split, trim, concat, regexp_replace
from common.createSpark import create_spark_session
from common.read_write_oracle import read_oracle

# 1. Initialize Spark Session
spark = create_spark_session("Report", "hdfs://10.0.17.76:8020")

# 2. Read and Parse HDFS Report
report = spark.read.format("text").load("hdfs://10.0.17.76:8020/reports/ba*")
report = report.filter(col("value").rlike("^[0-9]"))
report = report.withColumn("temp", split(trim(col("value")), "\s+")) \
    .select(
        col("temp").getItem(0).alias("GLCC"),
        col("temp").getItem(1).alias("BANCS"),
        col("temp").getItem(2).alias("GLIF"),
        col("temp").getItem(3).alias("DIFFERENCE"),
        col("temp").getItem(4).alias("DIFF_BW_YESTERDAY") 
    )

# 3. Read Oracle Data
difference_sql = f"(select * from DIFFERENCE) t"
oracle_db = read_oracle(spark, difference_sql)
oracle_db = oracle_db.select(
    concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")).alias("glcc"),
    col("CBS_BALANCE").alias("BANCS"),
    col("GL_BALANCE").alias("GLIF"),
    col("DIFFERENCE_AMOUNT").alias("DIFFERENCE"),
    col("DIFF_BW_YESTERDAY")
)

# 4. Clean Data and Perform Calculations
report = report \
    .withColumn("BANCS", F.regexp_replace("BANCS", "[,-]", "").cast("double").cast("long")) \
    .withColumn("GLIF", F.regexp_replace("GLIF", "[,-]", "").cast("double").cast("long")) \
    .withColumn("DIFFERENCE", F.regexp_replace("DIFFERENCE", "[,-]", "").cast("double").cast("long")) \
    .withColumn("DIFF_BW_YESTERDAY", F.regexp_replace("DIFF_BW_YESTERDAY", ",", ""))

# Math operation
report = report.withColumn("DIFF CALC", -col("BANCS") + col("GLIF"))
total_rows = report.count()
# --- EXPORT 1: Formatted .TXT (With Borders & Alignment) ---
# This uses Spark's internal 'showString' to create a visual table as a string
formatted_txt = report._jdf.showString(total_rows, 20, False)
with open("Final_Report.txt", "w") as f:
    f.write(formatted_txt)

# --- EXPORT 2: .CSV (For Excel) ---
# coalesce(1) ensures a single file is created instead of multiple parts
report.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://10.0.17.76:8020/reports/final_report_csv")

print("Reports generated: .txt (formatted) and .csv (HDFS)")
