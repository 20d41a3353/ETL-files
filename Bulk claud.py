import datetime 
from pyspark.sql import SparkSession, functions as F
import pyspark.sql.types as T

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import (
    to_date, expr, round, lower, col, when, length, trim, count,
    substring, concat, lit, create_map, to_timestamp, sum, broadcast,
    coalesce, current_timestamp, first, input_file_name
)
from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType, DateType, TimestampType
from datetime import date, timedelta, datetime
from pyspark.sql.functions import desc
import sys
from py4j.java_gateway import java_import
from delta.tables import DeltaTable 
import concurrent.futures
import builtins
from pyspark.sql import Row
import re   
from collections import defaultdict

# =========== Common Methods =============================
from common.logger import setup_logger 
from common.properties import get_oracle_properties
from common.dateUtil import get_etl_date
from common.fileUtil import files_to_read, paths_for_read, tables_to_read, get_hdfs_base, get_delta_path_by_file_type
from common.processRun import get_run_id, get_process_run_id
from common.pattern import read_patterns
from common.createSpark import create_spark_session
from common.check_monthend import check_MonthEnd, check_MonthEnd_minus1, check_Previous_MonthEnd
from common.batch_journal_id import get_batch_id, get_journal_id
from common.constants import Process
from common.Precheck import run_currency_precheck
from common.PPF_FCNB import ppf_postings, monthend_posting
from common.read_write_oracle import read_oracle, write_oracle
from common.provision1 import process_provision_data, reverse_provision


def build_column_expressions(metadata_rows):
    """
    Build all column expressions at once from metadata to avoid
    iterative withColumn chaining (which creates deeply nested logical plans).
    Returns a dict of {column_name: column_expr} for use in a single withColumns call.
    """
    exprs = {}
    for row in metadata_rows:
        if row["TOBEINCLUDED"] == "yes":
            column_name = row["COLUMNNAME"]
            start_pos = int(row["STARTVALUE"])
            end_pos   = int(row["ENDVALUE"]) - 1
            col_length = end_pos - start_pos + 1
            exprs[column_name] = trim(substring(col("value"), start_pos, col_length))
    return exprs


def main():
    print("hello")

    BASE_HDFS = get_hdfs_base()
    spark = create_spark_session("Bulk Data PIPELINE", BASE_HDFS)

    # ── Tune Spark for large fixed-width text workloads ──────────────────────
    spark.conf.set("spark.sql.shuffle.partitions", "400")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    # Avoid OOM from very wide logical plans during analysis
    spark.conf.set("spark.sql.optimizer.maxIterations", "200")
    # ─────────────────────────────────────────────────────────────────────────

    # ── 1. Fetch file names — avoid collect() on large result sets ───────────
    query = f"""(SELECT FILE_NAME FROM controll_file_master
                  WHERE ETL_DATE = '12-MAR-26'
                    AND FILE_TYPE = 'GLIF') e"""
    filenames_df = read_oracle(spark, query)

    # toPandas() is safe here: control table rows are always small
    filenames_pd = filenames_df.select("FILE_NAME").toPandas()
    my_list = filenames_pd["FILE_NAME"].tolist()

    # ── 2. Group files by stream letter ─────────────────────────────────────
    pattern = r'^(.*?)(\d{8})_([a-zA-Z])_.*$'
    grouped = defaultdict(list)
    for fname in my_list:
        match = re.match(pattern, fname)
        if match:
            stream = match.group(3).lower()
            grouped[stream].append(fname)

    all_paths = []
    for stream, files in grouped.items():
        paths = [f"{BASE_HDFS}/CBS-FILES/12-03-2026/GLIF/{stream}/{ft}.gz" for ft in files]
        all_paths.extend(paths)

    print(f"Total paths to read: {len(all_paths)}")

    # ── 3. Read raw data ─────────────────────────────────────────────────────
    # Repartition immediately after load so downstream stages don't operate
    # on hundreds of tiny gz splits all at once.
    df_raw = (
        spark.read.format("text")
        .load(all_paths)
        .withColumn("SourcePath", input_file_name())
        .withColumn("count", lit(1))
        .repartition(400)          # tune to cluster size / data volume
    )

    # ── 4. Fetch column metadata — small table, toPandas() is fine ──────────
    query_parm = f"""(SELECT * FROM TOTAL_DATA_PARAMETERS WHERE FILE_TYPE = 'GLIF') e"""
    parameters_df = read_oracle(spark, query_parm)
    metadata = parameters_df.toPandas().to_dict("records")   # list of dicts, no JVM collect()

    # ── 5. Apply ALL fixed-width extractions in ONE withColumns call ─────────
    # Chaining 30-40 withColumn() calls creates a deeply nested logical plan
    # that blows the driver heap during analysis/optimization.
    col_exprs = build_column_expressions(metadata)
    transformed_df = df_raw.withColumns(col_exprs).drop("value")

    # ── 6. Business logic (unchanged) ────────────────────────────────────────
    df_processed = transformed_df.withColumns({
        "Id":            concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")),
        "currency_code": col("CURRENCY_IND")
    })

    df_clean = df_processed.withColumn(
        "Amount_raw",
        when(col("currency_code") != "INR", col("FOREIGN_AMOUNT"))
        .otherwise(col("AMOUNT"))
    )

    df_clean = df_clean.withColumns({
        "Amount_base": substring(col("Amount_raw"), 1, 16),
        "sign_char":   substring(col("Amount_raw"), 17, 1)
    })

    digit_map = create_map(
        lit('1'), lit('1'), lit('2'), lit('2'), lit('3'), lit('3'), lit('4'), lit('4'), lit('5'), lit('5'),
        lit('6'), lit('6'), lit('7'), lit('7'), lit('8'), lit('8'), lit('9'), lit('9'), lit('0'), lit('0'),
        lit('p'), lit('0'), lit('q'), lit('1'), lit('r'), lit('2'), lit('s'), lit('3'), lit('t'), lit('4'),
        lit('u'), lit('5'), lit('v'), lit('6'), lit('w'), lit('7'), lit('x'), lit('8'), lit('y'), lit('9')
    )

    df_with_signed_amount = df_clean.withColumns({
        "last_digit": digit_map.getItem(col("sign_char")),
        "sign": when(
            col("sign_char").isin(['p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y']), -1
        ).otherwise(1)
    })

    df_with_signed_amount = df_with_signed_amount.withColumn(
        "Amount_decimal_str",
        concat(col("Amount_base"), col("last_digit"))
    )

    df_final = df_with_signed_amount.withColumn(
        "Amount_final",
        (col("Amount_decimal_str").cast(DecimalType(25, 4)) / 1000) * col("sign")
    )

    # ── 7. Select final columns ───────────────────────────────────────────────
    final_columns = [
        "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BRANCH_CODE", "CGL", "CURRENCY",
        "CURRENCY_IND", "DESCRIPTION", "FOREIGN_AMOUNT", "JOURNAL_NUMBER",
        "KEY_1", "POST_DATE", "TIME", "TRANSACTION_CODE", "TRANSACTION_DATE1",
        "TRANSACTION_DATE2", "TRANSACTION_TYPE", "Amount_raw", "Amount_final"
    ]
    df_final = df_final.select(*final_columns)

    # ── 8. Write with partitioning to avoid single large task OOM ────────────
    # Coalesce to a reasonable number of output files; tune based on data size.
    # Rule of thumb: ~256-512 MB per output file.
    output_path = "hdfs://10.0.17.76/GLIF/"

    (
        df_final
        .coalesce(200)                      # avoid thousands of tiny Delta files
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")  # safe for overwrite; prevents schema mismatch errors
        .save(output_path)
    )

    print("Write complete.")


if __name__ == "__main__":
    main()

