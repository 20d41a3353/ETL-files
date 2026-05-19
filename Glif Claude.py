import datetime
from pyspark.sql import SparkSession, functions as F
import pyspark.sql.types as T
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType,
    DecimalType, LongType, TimestampType
)
from pyspark.sql.functions import (
    to_date, expr, round, lower, col, when, length, count,
    substring, trim, concat, lit, create_map, to_timestamp,
    sum, broadcast, coalesce, current_timestamp, first, desc
)
from datetime import date, timedelta, datetime
import sys
from py4j.java_gateway import java_import
from delta.tables import DeltaTable
import concurrent.futures
import builtins
from pyspark.sql import Row
import re

# =========== Common Methods =============================
from common.logger import setup_logger
from common.properties import get_oracle_properties
from common.dateUtil import get_etl_date
from common.fileUtil import files_to_read
from common.fileUtil import paths_for_read
from common.fileUtil import tables_to_read
from common.processRun import get_run_id
from common.pattern import read_patterns
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from common.check_monthend import check_MonthEnd
from common.check_monthend import check_MonthEnd_minus1
from common.check_monthend import check_Previous_MonthEnd
from common.batch_journal_id import get_batch_id
from common.batch_journal_id import get_journal_id
from common.constants import Process
from common.Precheck import run_currency_precheck
from common.log_etl_modified import log_etl
from common.PPF_FCNB import ppf_postings
from common.PPF_FCNB import monthend_posting
from common.read_write_oracle import read_oracle
from common.read_write_oracle import write_oracle
from common.provision1 import process_provision_data
from common.provision1 import reverse_provision
from common.fileUtil import get_delta_path_by_file_type
from common.processRun import get_process_run_id


def main(args):

    # ===================================================================================
    #  STEP 1: Spark Session + Date Setup
    # ===================================================================================
    HDFS_BASE = get_hdfs_base()
    spark = create_spark_session("GLIF_PIPELINE", HDFS_BASE)

    # Fetch ETL date ONCE and reuse everywhere
    today_date_obj = get_etl_date(spark)         # keep as date object for logic checks
    todayYearEnd   = today_date_obj
    yesterday_date = today_date_obj - timedelta(days=1)

    posting_date_str = today_date_obj.strftime("%Y-%m-%d")
    today_str        = posting_date_str
    yesterday        = yesterday_date.strftime("%Y-%m-%d")
    today            = posting_date_str
    etl_date_path    = today_date_obj.strftime("%Y%m%d")   # reused later for manifest path

    PROCESS_ID = Process.GLIF_PIPELINE
    RUN_ID     = get_run_id(spark, today_date_obj, PROCESS_ID)
    logger     = setup_logger("GLIF", RUN_ID, posting_date_str)

    logger.info(f"=== Using current date for POST_DATE: {posting_date_str} ===")
    logger.info(f"RUN_ID as : {RUN_ID}")

    # ===================================================================================
    #  STEP 2: Oracle Properties — fetch ONCE, reuse everywhere
    # ===================================================================================
    oracle_properties = get_oracle_properties()
    logger.info(f"Fetched oracle properties SUCCESSFULLY :)")
    logger.info(oracle_properties)

    oracle_url      = oracle_properties["url"]
    oracle_user     = oracle_properties["user"]
    oracle_password = oracle_properties["password"]
    oracle_driver   = oracle_properties["driver"]

    startTime = datetime.now()

    # ===================================================================================
    #  STEP 3: process_run_id + initial ETL log
    # ===================================================================================
    process_run_id = sys.argv[1] if len(sys.argv) > 1 else get_process_run_id(spark, PROCESS_ID, today)
    log_etl(spark, process_run_id, "glif_started", 1)

    # ===================================================================================
    #  STEP 4: Read FCNB lookup tables from Oracle (done early, reused later)
    # ===================================================================================
    fncb_cgl_query      = "(SELECT FROM_CGL,TO_CGL,CGL_TYPE FROM FCNB_CGL) T1"
    fcnb_cgl_df         = read_oracle(spark, fncb_cgl_query)

    fcnb_currency_query = "(SELECT CURRENCY_CODE,DEPOSIT_EXPENSE_FLAG,LOAN_INCOME_FLAG FROM FCNB_CURRENCY_CONFIG) T1"
    fcnb_currency       = read_oracle(spark, fcnb_currency_query)

    # ===================================================================================
    #  STEP 5: Get BATCH_ID
    # ===================================================================================
    BATCH_ID_LITERAL = None
    try:
        BATCH_ID_LITERAL = get_batch_id(spark)
    except Exception as e:
        print(f"Error fetching BATCH_SEQ via JDBC in gl : {e}")
        spark.stop()
        sys.exit(1)

    # ===================================================================================
    #  STEP 6: Hadoop FileSystem setup
    # ===================================================================================
    sc  = spark.sparkContext
    jvm = sc._jvm
    jsc = sc._jsc
    fs  = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    Path = jvm.org.apache.hadoop.fs.Path

    # ===================================================================================
    #  STEP 7: Resolve all paths and table names
    # ===================================================================================
    hdfs_paths = paths_for_read("glif", today_date_obj, today_str)

    HDFS_BASE          = hdfs_paths["HDFS_BASE"]
    HDFS_PATH          = hdfs_paths["HDFS_PATH"]
    GL_DATALAKE_PATH   = hdfs_paths["GL_DATALAKE_PATH"]
    manifest_filename  = hdfs_paths["manifest_filename"]
    spark_stream_path  = hdfs_paths["spark_stream_path"] + manifest_filename
    BASE_MANIFEST_PATH = hdfs_paths["MANIFEST_BASE_PATH"]

    oracle_tables = tables_to_read("glif")

    TRANSACTIONS = oracle_tables["TRANSACTIONS"]
    BALANCE      = oracle_tables["BALANCE"]
    INVALID      = oracle_tables["INVALID"]
    CGLS         = oracle_tables["CGLS"]
    CURRENCY     = oracle_tables["CURRENCY"]
    BRANCH       = oracle_tables["BRANCH"]

    logger.info(f"ETL date : {today}")
    logger.info(f"Oracle properties fetched successfully")

    GLIF_DELTA_PATH = get_delta_path_by_file_type("glif", today_date_obj)

    # ===================================================================================
    #  STEP 8: RUN_ID > 1 — Delete existing data (Oracle + Delta Lake)
    #          OPTIMIZATION: Single JDBC connection for all 3 Oracle DELETEs
    # ===================================================================================
    if RUN_ID > 1:
        log_etl(spark, process_run_id, "GLIF_deletion_oracle", 1, "In GLIF deletion started")

        # --- Single connection, batch all 3 Oracle DELETEs ---
        conn  = None
        stmt  = None
        try:
            conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
                oracle_url, oracle_user, oracle_password
            )
            conn.setAutoCommit(False)
            stmt = conn.createStatement()

            # DELETE 1: BALANCE table
            stmt.execute(
                f"DELETE FROM {BALANCE} WHERE BALANCE_DATE = TO_DATE('{posting_date_str}','yyyy-mm-dd')"
            )
            logger.info(f"Deleted from {BALANCE} table successfully")

            # DELETE 2: TRANSACTIONS table (specific SOURCE_FLAGs)
            stmt.execute(
                f"""DELETE FROM {TRANSACTIONS}
                    WHERE TRANSACTION_DATE = TO_DATE('{posting_date_str}', 'yyyy-mm-dd')
                      AND SOURCE_FLAG IN ('C', 'F', 'P', 'T', 'V')"""
            )
            logger.info(f"Deleted from {TRANSACTIONS} table successfully")

            # DELETE 3: INVALID table
            stmt.execute(
                f"""DELETE FROM {INVALID}
                    WHERE TRANSACTION_DATE = TO_DATE('{posting_date_str}', 'yyyy-mm-dd')"""
            )
            logger.info(f"Deleted from {INVALID} table successfully")

            conn.commit()
            log_etl(spark, process_run_id, "GLIF_deletion_oracle", 2, "In GLIF deletion success")

        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"Error {e} while deleting from Oracle tables")
            log_etl(spark, process_run_id, "GLIF_deletion_oracle", 3, "In GLIF deletion failed")
            spark.stop()
            sys.exit(1)
        finally:
            if stmt:
                try:
                    stmt.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        # --- Delete from Delta Lake ---
        try:
            log_etl(spark, process_run_id, "GLIF_deletion_datalake", 1, "In GLIF deletion datalake started")
            delta_table = DeltaTable.forPath(spark, GL_DATALAKE_PATH)
            delta_table.delete(f"BALANCE_DATE = '{posting_date_str}'")
            log_etl(spark, process_run_id, "GLIF_deletion_datalake", 2, "Difference deletion datalake completed")
        except Exception as e:
            logger.info(f"Error during saving in datalake  {e}")
            log_etl(spark, process_run_id, "GLIF_deletion_datalake", 3, "Error in Difference deletion")
            log_etl(spark, process_run_id, "file_reading_Started", 3, "Error in deleting difference to delta lake")
            log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
            spark.stop()
            sys.exit(1)

    # ===================================================================================
    #  STEP 9: Read raw GLIF Delta data
    # ===================================================================================
    try:
        log_etl(spark, process_run_id, "file_reading_started", 1, "File Reading Started (Delta)")
        df_raw = spark.read.format("delta").load(GLIF_DELTA_PATH)
        print(f"Reading from {GLIF_DELTA_PATH}")
        # Exclude glifpri and glifprb files — business rule preserved
        df_raw = df_raw.filter(
            (~lower(col("file_name")).startswith("glifpri")) &
            (~lower(col("file_name")).startswith("glifprb"))
        )
    except Exception as e:
        logger.info(f"Error reading files... {e}")
        log_etl(spark, process_run_id, "file_reading_started", 3, "ERROR READING FILES : 201 : 411")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # ===================================================================================
    #  STEP 10: Amount parsing — consolidated into a single transformation chain
    #           (same logic, fewer intermediate variables)
    # ===================================================================================
    logger.info(f"Reading Patterns")

    digit_map = create_map(
        lit('1'), lit('1'), lit('2'), lit('2'), lit('3'), lit('3'), lit('4'), lit('4'), lit('5'), lit('5'),
        lit('6'), lit('6'), lit('7'), lit('7'), lit('8'), lit('8'), lit('9'), lit('9'), lit('0'), lit('0'),
        lit('p'), lit('0'), lit('q'), lit('1'), lit('r'), lit('2'), lit('s'), lit('3'), lit('t'), lit('4'),
        lit('u'), lit('5'), lit('v'), lit('6'), lit('w'), lit('7'), lit('x'), lit('8'), lit('y'), lit('9')
    )

    df_with_signed_amount = (
        df_raw
        .withColumn("Id",           concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")))
        .withColumn("currency_code", col("CURRENCY_IND"))
        .withColumn("Amount_raw",
            when(col("currency_code") != "INR", col("FOREIGN_AMOUNT"))
            .otherwise(col("AMOUNT"))
        )
        .withColumn("Amount_base",  substring(col("Amount_raw"), 1, 16))
        .withColumn("sign_char",    substring(col("Amount_raw"), 17, 1))
        .withColumn("last_digit",   digit_map.getItem(col("sign_char")))
        .withColumn("sign",
            when(col("sign_char").isin(['p','q','r','s','t','u','v','w','x','y']), -1)
            .otherwise(1)
        )
        .withColumn("Amount_decimal_str", concat(col("Amount_base"), col("last_digit")))
        .withColumn("Amount_final",
            (col("Amount_decimal_str").cast(DecimalType(25, 4)) / 1000) * col("sign")
        )
        .withColumn("Amountpve", when(col("Amount_final") > 0, col("Amount_final")).otherwise(0))
        .withColumn("Amountnve", when(col("Amount_final") < 0, col("Amount_final")).otherwise(0))
    )

    # ===================================================================================
    #  STEP 11: Aggregate by Id (BRANCH+CURRENCY+CGL)
    # ===================================================================================
    try:
        df_agg = df_with_signed_amount.groupBy("Id").agg(
            sum(col("Amountpve")).alias("CREDIT_AMOUNT"),
            sum(col("Amountnve")).alias("DEBIT_AMOUNT"),
            count(col("Id")).alias("TRANSACTION_COUNT")
        )
    except Exception as e:
        logger.info(f"Error reading files... {e}")
        log_etl(spark, process_run_id, "file_reading_started", 3, "ERROR in grpby   : 202")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # ===================================================================================
    #  STEP 12: Map aggregated columns
    # ===================================================================================
    df_mapped = df_agg.withColumns({
        "BATCH_ID":    lit(BATCH_ID_LITERAL).cast(StringType()),
        "JOURNAL_ID":  lit(None).cast(StringType()),
        "BRANCH_CODE": substring(col("Id"), 1, 5),
        "CURRENCY":    substring(col("Id"), 6, 3),
        "CGL":         substring(col("Id"), 9, 10),
        "NARRATION":   lit("CBS txns").cast(StringType()),
        "SOURCE_FLAG": lit("C").cast(StringType())
    })

    df_final_schema = df_mapped.select(
        "BATCH_ID", "JOURNAL_ID", "BRANCH_CODE", "CURRENCY", "CGL",
        "NARRATION", "CREDIT_AMOUNT", "DEBIT_AMOUNT", "TRANSACTION_COUNT", "SOURCE_FLAG"
    )
    logger.info(f"Raw Data Got Filtered")

    # ===================================================================================
    #  STEP 13: Month-end Reverse Provision (RUN_ID == 1 only)
    #           Business logic preserved exactly
    # ===================================================================================
    if check_MonthEnd():
        if RUN_ID == 1:
            try:
                log_etl(spark, process_run_id, "reverse_provision", 1, "Reverse Provision Start")
                logger.info("Reverse Provision Started")
                final_df = reverse_provision(TRANSACTIONS, BATCH_ID_LITERAL).withColumn(
                    "TRANSACTION_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd")
                )
                try:
                    write_oracle(final_df, TRANSACTIONS)
                except Exception as e:
                    log_etl(spark, process_run_id, "reverse_provision", 3, "Error in Reverse Provision transaction level")
                    logger.info(f"Error {e} while writing into GL_TRANSACTIONS")

                try:
                    log_etl(spark, process_run_id, "reverse_provision", 2, "Reverse Provision completed")
                except Exception as e:
                    logger.info(f"Error {e} while merging reversal transactions")
                    log_etl(spark, process_run_id, "reverse_provision", 3, "Error in Reverse Provision transaction level")

            except Exception as e:
                log_etl(spark, process_run_id, "reverse_provision", 3, "Error in Reverse Provision")
                logger.error(f"Error {e} while loading or writing of reversed provision transactions")

    # ===================================================================================
    #  STEP 14: Month-end Provision — union with main df
    # ===================================================================================
    if check_MonthEnd():
        provision_df    = process_provision_data(RUN_ID, BATCH_ID_LITERAL)
        df_final_schema = df_final_schema.unionByName(provision_df)

    df_final_schema.cache()

    # ===================================================================================
    #  STEP 15: Load master lookup tables from Oracle IN PARALLEL
    #           (branch, currency, CGL — previously sequential, now concurrent)
    # ===================================================================================
    branch_query = f"(SELECT code FROM {BRANCH}) T1"
    currency_query = f"(SELECT CURRENCY_CODE, CURRENCY_RATE FROM {CURRENCY}) T1"
    cgl_query = f"(SELECT CGL_NUMBER FROM {CGLS} WHERE STATUS = '1') T1"

    def _jdbc_read(query):
        return (
            spark.read.format("jdbc")
            .option("url",      oracle_url)
            .option("dbtable",  query)
            .option("user",     oracle_user)
            .option("password", oracle_password)
            .option("driver",   oracle_driver)
            .load()
        )

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            f_branch   = executor.submit(_jdbc_read, branch_query)
            f_currency = executor.submit(_jdbc_read, currency_query)
            f_cgl      = executor.submit(_jdbc_read, cgl_query)

            branch_list      = f_branch.result()
            currency_list    = f_currency.result()
            master_cgl_list  = f_cgl.result()

        logger.info("Branch, Currency and CGL master lists loaded successfully (parallel)")

    except Exception as e:
        logger.error(f"Error {e} while fetching master tables from oracle.")
        log_etl(spark, process_run_id, "file_reading_started", 3, "ERROR in datafetching master tables")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # ===================================================================================
    #  STEP 16: Branch validation
    # ===================================================================================
    try:
        df_validated_branch = df_final_schema.join(
            broadcast(
                branch_list.select(col("CODE").alias("BRANCH_CODE"))
                           .withColumn("IS_VALID_BRANCH", lit(1))
            ),
            on="BRANCH_CODE",
            how="left"
        )
    except Exception as e:
        logger.error(f"Error {e} while joining with branch code")
        log_etl(spark, process_run_id, "file_reading_started", 3, "ERROR in branchcode join")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    df_invalid_branches = df_validated_branch.filter(col("IS_VALID_BRANCH").isNull()) \
        .withColumn("REASON", lit("INVALID BRANCH")) \
        .select(
            "BRANCH_CODE", "BATCH_ID", "CURRENCY", "CGL", "NARRATION",
            "CREDIT_AMOUNT", "DEBIT_AMOUNT", "TRANSACTION_COUNT", "SOURCE_FLAG", "REASON"
        )

    df_valid_branches = df_validated_branch.filter(col("IS_VALID_BRANCH").isNotNull())

    # ===================================================================================
    #  STEP 17: Currency validation
    # ===================================================================================
    df_validated_currency = df_valid_branches.join(
        broadcast(
            currency_list.select(col("CURRENCY_CODE").alias("CURRENCY"))
                         .withColumn("IS_VALID_CURRENCY", lit(1))
        ),
        on="CURRENCY",
        how="left"
    ).withColumn(
        "VALIDATED_CURRENCY",
        when(col("IS_VALID_CURRENCY").isNotNull(), col("CURRENCY"))
        .otherwise(lit("invalid_CURRENCY"))
    )

    df_invalid_currency = df_validated_currency.where(
        col("VALIDATED_CURRENCY") == "invalid_CURRENCY"
    ).withColumn("REASON", lit("INVALID CURRENCY")).select(
        "BRANCH_CODE", "BATCH_ID", "CURRENCY", "CGL", "NARRATION",
        "CREDIT_AMOUNT", "DEBIT_AMOUNT", "TRANSACTION_COUNT", "SOURCE_FLAG", "REASON"
    )

    df_valid_currency = df_validated_currency.where(
        col("VALIDATED_CURRENCY") != "invalid_CURRENCY"
    ).drop(col("VALIDATED_CURRENCY"), col("IS_VALID_CURRENCY"))

    # Combine invalid records + add TRANSACTION_DATE — business logic preserved
    invalid_df = df_invalid_branches.unionByName(df_invalid_currency) \
        .withColumn("TRANSACTION_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))

    # ===================================================================================
    #  STEP 18: CGL validation + remapping (5xxxxxxx → 5000000000, else 1111111111)
    # ===================================================================================
    try:
        df_validated = df_valid_currency.join(
            broadcast(
                master_cgl_list.select(col("CGL_NUMBER").alias("CGL"))
                               .withColumn("IS_VALID", lit(1))
            ),
            on="CGL",
            how="left"
        ).withColumn(
            "VALIDATED_CGL",
            when(col("IS_VALID").isNotNull(), col("CGL"))
            .when(col("CGL").startswith("5"), lit("5000000000"))
            .otherwise(lit("1111111111"))
        ).withColumn(
            "NARRATION1",
            when(col("IS_VALID").isNull(),
                 concat(col("NARRATION"), lit("- INVALID-"), col("CGL")))
            .otherwise(col("NARRATION"))
        )
    except Exception as e:
        logger.error(f"Error {e} while joining cgl")
        log_etl(spark, process_run_id, "file_reading_started", 3, "Error in joining cgl")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # Unpersist df_final_schema — no longer needed after CGL join
    df_final_schema.unpersist()

    try:
        result = df_validated.groupBy(
            "VALIDATED_CGL", "CURRENCY", "BRANCH_CODE", "SOURCE_FLAG"
        ).agg(
            sum("CREDIT_AMOUNT").alias("CREDIT_AMOUNT"),
            sum("DEBIT_AMOUNT").alias("DEBIT_AMOUNT"),
            sum("TRANSACTION_COUNT").alias("TRANSACTION_COUNT"),
            first("NARRATION1").alias("NARRATION"),
            first("BATCH_ID").alias("BATCH_ID"),
            first("JOURNAL_ID").alias("JOURNAL_ID"),
        ).select(
            col("VALIDATED_CGL").alias("CGL"), "CURRENCY", "BRANCH_CODE", "NARRATION",
            "SOURCE_FLAG", "BATCH_ID", "CREDIT_AMOUNT", "DEBIT_AMOUNT",
            "TRANSACTION_COUNT", "JOURNAL_ID"
        )
    except Exception as e:
        logger.error(f"Error {e} while grouping final validated dataframe")
        log_etl(spark, process_run_id, "file_reading_started", 3, "Error in grping final validated dataframe.")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # ===================================================================================
    #  STEP 19: Balancing and PPF Logic
    #           Business logic preserved exactly
    # ===================================================================================
    result_net1 = result.withColumn(
        "check",
        when(substring(col("CGL"), 1, 1) == "5", lit("5000000000"))
        .otherwise(lit("1111111111"))
    )

    try:
        new_result = result_net1.groupBy(
            "BRANCH_CODE", "CURRENCY", col("check").alias("CGL"), "SOURCE_FLAG"
        ).agg(
            sum("CREDIT_AMOUNT").alias("CREDIT_AMOUNT"),
            sum("DEBIT_AMOUNT").alias("DEBIT_AMOUNT"),
            first("NARRATION").alias("NARRATION"),
            first("BATCH_ID").alias("BATCH_ID"),
            first("JOURNAL_ID").alias("JOURNAL_ID"),
            sum("TRANSACTION_COUNT").alias("TRANSACTION_COUNT")
        ).withColumn(
            "NET", col("CREDIT_AMOUNT") + col("DEBIT_AMOUNT")
        )
    except Exception as e:
        logger.error(f"Error {e} while grouping Balancing and PPF Logic")
        log_etl(spark, process_run_id, "file_reading_started", 3, "Error grpby of Balancing and PPF Logic")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # Synthetic balancing entries — business logic preserved exactly
    synthetic = new_result.filter(col("NET") != 0).select(
        col("CGL"), col("CURRENCY"), col("BRANCH_CODE"),
        when(col("NET") < 0, -col("NET")).otherwise(lit(0)).alias("CREDIT_AMOUNT"),
        when(col("NET") > 0, -col("NET")).otherwise(lit(0)).alias("DEBIT_AMOUNT"),
        col("TRANSACTION_COUNT"),
        lit("OUT OF BAL").alias("NARRATION"),
        col("SOURCE_FLAG"),
        col("BATCH_ID"),
        col("JOURNAL_ID")
    )

    try:
        final_balanced = result.unionByName(synthetic)
    except Exception as e:
        logger.error(f"Error {e} in finding the synthetic dataframe")
        log_etl(spark, process_run_id, "file_reading_started", 3, "Error in finding the synthetic df")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # Materialize final_balanced before PPF (used in multiple downstream steps)
    final_balanced.cache()
    final_balanced.count()

    # ===================================================================================
    #  STEP 20: PPF Postings
    # ===================================================================================
    try:
        journal_id = get_journal_id(spark)
    except Exception as e:
        print(f"Error fetching JOURNAL_SEQ via JDBC in gl : {e}")
        spark.stop()
        sys.exit(1)

    PPF_Posting    = ppf_postings(final_balanced).withColumn("JOURNAL_ID", lit(journal_id))
    PPF_Posting_df = final_balanced.unionByName(PPF_Posting)
    PPF_Posting    = PPF_Posting_df.withColumn(
        "TRANSACTION_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd")
    )

    # Materialize PPF_Posting — used in multiple writes downstream
    PPF_Posting.cache()
    PPF_Posting.count()

    # Unpersist final_balanced — no longer needed
    final_balanced.unpersist()

    # ===================================================================================
    #  STEP 21: Journal Entries from DB (SOURCE_FLAG J, R)
    #           Business logic preserved exactly
    # ===================================================================================
    if RUN_ID > 0:
        sql_query = f"""(
            SELECT BRANCH_CODE, CURRENCY, CGL, CREDIT_AMOUNT, DEBIT_AMOUNT,
                   BATCH_ID, JOURNAL_ID, NARRATION, TRANSACTION_COUNT, SOURCE_FLAG
            FROM {TRANSACTIONS}
            WHERE TRUNC(TRANSACTION_DATE) = TO_DATE('{posting_date_str}', 'YYYY-MM-DD')
              AND SOURCE_FLAG IN ('J','R')
        ) T1"""
        try:
            journal_entries = read_oracle(spark, sql_query)
            logger.info(f"Journal entries loaded from db for {posting_date_str} date.")
        except Exception as e:
            logger.error(f"Error {e} while loading Journal entries for {posting_date_str} date.")
            log_etl(spark, process_run_id, "file_reading_started", 3, "Error in fetching journal transactions")
            log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
            spark.stop()
            sys.exit(1)

        PPF_Posting_df = PPF_Posting_df.unionByName(journal_entries).groupBy(
            "CGL", "BRANCH_CODE", "CURRENCY", "SOURCE_FLAG"
        ).agg(
            sum(col("CREDIT_AMOUNT")).alias("CREDIT_AMOUNT"),
            sum(col("DEBIT_AMOUNT")).alias("DEBIT_AMOUNT"),
            sum(col("TRANSACTION_COUNT")).alias("TRANSACTION_COUNT"),
            first("NARRATION").alias("NARRATION"),
            first("BATCH_ID").alias("BATCH_ID"),
            first("JOURNAL_ID").alias("JOURNAL_ID"),
        ).select(
            "BRANCH_CODE", "CURRENCY", "CGL", "CREDIT_AMOUNT", "DEBIT_AMOUNT",
            "BATCH_ID", "JOURNAL_ID", "NARRATION", "TRANSACTION_COUNT", "SOURCE_FLAG"
        )

    # ===================================================================================
    #  STEP 22: Compute today's BALANCE from transactions
    # ===================================================================================
    processed_df = PPF_Posting_df.withColumn(
        "BALANCE", col("CREDIT_AMOUNT") + col("DEBIT_AMOUNT")
    ).select("CGL", "BALANCE", "CURRENCY", "BRANCH_CODE")

    processed_df = processed_df.select(
        concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")).alias("glcc"),
        col("BALANCE")
    )

    # ===================================================================================
    #  STEP 23: Load previous closing balance from Delta Lake or Oracle fallback
    #           Business logic preserved exactly (year-start, RUN_ID==1, RUN_ID>1)
    # ===================================================================================
    is_year_start = (todayYearEnd.month == 4 and todayYearEnd.day == 1)

    if not is_year_start:
        if RUN_ID == 1:
            try:
                print(f"Reading from {GL_DATALAKE_PATH}")
                df_gl = spark.read.format("delta").load(GL_DATALAKE_PATH) \
                    .filter(col("BALANCE_DATE") == today_str) \
                    .select(col("glcc"), col("closing_balance").cast(DecimalType(25, 4)).alias("BALANCE"))
                logger.info(f"Data loaded from deltalake.")
            except Exception as e:
                logger.error(f"Error {e} while loading data for {today_str} from delta lake")
                log_etl(spark, process_run_id, "file_reading_started", 3, "error in loading today data from delta lake")
                try:
                    BALANCE_query = f"""(SELECT CGL, CURRENCY, BRANCH_CODE, BALANCE
                                         FROM GL_BALANCE
                                         WHERE BALANCE_DATE = TO_DATE('{today_str}', 'YYYY-MM-DD')) a"""
                    gl_yesterday  = read_oracle(spark, BALANCE_query)
                    print("yesterday gl")
                    df_gl = gl_yesterday.select(
                        concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")).alias("glcc"),
                        col("BALANCE")
                    )
                except Exception as e:
                    df_gl = spark.createDataFrame([], schema="glcc STRING, BALANCE DECIMAL(25,4)")
                    logger.error(f"Error {e} while loading data from delta lake.")
                    log_etl(spark, process_run_id, "file_reading_started", 3, "error in loading today data in oracle")
                    log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
                    spark.stop()
        else:
            try:
                df_gl = spark.read.format("delta").load(GL_DATALAKE_PATH) \
                    .filter(col("BALANCE_DATE") == yesterday) \
                    .select(col("glcc"), col("closing_balance").cast(DecimalType(25, 4)).alias("BALANCE"))
                logger.info(f"Data loaded from deltalake.")
            except Exception as e:
                logger.error(f"Error {e} while loading previous day {yesterday} data from delta lake")
                log_etl(spark, process_run_id, "file_reading_started", 3, "error in loading previous day data from delta lake")
                try:
                    BALANCE_query = f"""(SELECT CGL, CURRENCY, BRANCH_CODE, BALANCE
                                         FROM GL_BALANCE
                                         WHERE BALANCE_DATE = TO_DATE('{yesterday}', 'YYYY-MM-DD')) a"""
                    gl_yesterday  = read_oracle(spark, BALANCE_query)
                    print("yesterday gl")
                    df_gl = gl_yesterday.select(
                        concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")).alias("glcc"),
                        col("BALANCE")
                    )
                except Exception as e:
                    df_gl = spark.createDataFrame([], schema="glcc STRING, BALANCE DECIMAL(25,4)")
                    logger.error(f"Error {e} while loading data from delta lake.")
                    log_etl(spark, process_run_id, "file_reading_started", 3, "error in loading previous day data from oracle")
                    log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
                    spark.stop()
    else:
        # Year-start: read from GL_BALANCE_YEAR_END — business rule preserved
        BALANCE_query = f"""(SELECT CGL, CURRENCY, BRANCH_CODE, BALANCE
                              FROM GL_BALANCE_YEAR_END
                              WHERE BALANCE_DATE = TO_DATE('{posting_date_str}', 'YYYY-MM-DD')) a"""
        gl_yesterday  = read_oracle(spark, BALANCE_query)
        print("yesterday gl")
        df_gl = gl_yesterday.select(
            concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")).alias("glcc"),
            col("BALANCE")
        )

    # ===================================================================================
    #  STEP 24: Union today's movements with previous closing balance
    # ===================================================================================
    try:
        df_gl.cache()
        print(f'count of yesterdays df : {df_gl.count()}')
        combined_df = processed_df.unionByName(df_gl)
    except Exception as e:
        logger.error(f"Error {e} in union")
        log_etl(spark, process_run_id, "file_reading_started", 3, "error in doing union of current balance to closing balance")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    final_aggregated_df = combined_df.groupBy("glcc").agg(
        sum("BALANCE").alias("BALANCE")
    ).withColumn("BALANCE_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))

    dl_df = final_aggregated_df.select(
        "glcc",
        col("BALANCE").alias("closing_balance").cast(DecimalType(25, 4)),
        "BALANCE_DATE"
    )

    # Unpersist df_gl — no longer needed
    df_gl.unpersist()

    # ===================================================================================
    #  STEP 25: Month-end FCNB postings
    #           Business logic preserved exactly
    # ===================================================================================
    monthend = check_MonthEnd()
    logger.info(f"monthend is {monthend}")

    # Define FCNB_transactions with proper schema as fallback (avoids empty StructType issue)
    fcnb_schema = StructType([
        StructField("BRANCH_CODE",       StringType(),         True),
        StructField("CURRENCY",          StringType(),         True),
        StructField("CGL",               StringType(),         True),
        StructField("CREDIT_AMOUNT",     DecimalType(25, 4),   True),
        StructField("DEBIT_AMOUNT",      DecimalType(25, 4),   True),
        StructField("NARRATION",         StringType(),         True),
        StructField("SOURCE_FLAG",       StringType(),         True),
        StructField("BATCH_ID",          StringType(),         True),
        StructField("JOURNAL_ID",        StringType(),         True),
        StructField("TRANSACTION_COUNT", LongType(),           True),
        StructField("TRANSACTION_DATE",  DateType(),           True),
    ])
    FCNB_transactions = spark.createDataFrame([], fcnb_schema)

    if monthend:
        logger.info("Start FCNB Monthend")
        try:
            FCNB_journal_id = get_journal_id(spark)
        except Exception as e:
            print(f"Error {e} while fetching JOURNAL_SEQUENCE")

        FCNB_transactions = (
            monthend_posting(dl_df, fcnb_cgl_df, fcnb_currency, currency_list, posting_date_str)
            .withColumn("JOURNAL_ID", lit(FCNB_journal_id))
            .withColumn("BATCH_ID",   lit(BATCH_ID_LITERAL))
        )

        FCNB_result = (
            FCNB_transactions
            .withColumn("BALANCE", col("CREDIT_AMOUNT") + col("DEBIT_AMOUNT"))
            .select(
                concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")).alias("glcc"),
                col("BALANCE").alias("closing_balance")
            )
            .withColumn("BALANCE_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))
        )
        dl_df = dl_df.unionAll(FCNB_result)

    # ===================================================================================
    #  STEP 26: Final closing balance aggregation
    # ===================================================================================
    dl_df = dl_df.groupBy("glcc").agg(
        sum("closing_balance").alias("BALANCE"),
        first("BALANCE_DATE").alias("BALANCE_DATE")
    ).select("glcc", "BALANCE", "BALANCE_DATE")

    df_gl_balance_final = dl_df.withColumns({
        "BRANCH_CODE": substring(col("glcc"), 1, 5),
        "CURRENCY":    substring(col("glcc"), 6, 3),
        "CGL":         substring(col("glcc"), 9, 10),
    }).select("BRANCH_CODE", "CURRENCY", "CGL", "BALANCE_DATE", "BALANCE")

    # Materialize — used in currency conversion join below
    df_gl_balance_final.cache()
    df_gl_balance_final.count()

    # ===================================================================================
    #  STEP 27: Currency conversion to INR
    #           Business logic preserved exactly
    # ===================================================================================
    df_converted = (
        df_gl_balance_final
        .join(broadcast(currency_list), df_gl_balance_final["CURRENCY"] == currency_list["CURRENCY_CODE"], "left")
        .select("BRANCH_CODE", "CURRENCY", "CGL", "BALANCE_DATE", "BALANCE", "CURRENCY_RATE")
        .withColumn(
            "APPLIED_RATE",
            when(col("CURRENCY") == "INR", lit(1))
            .otherwise(coalesce(col("CURRENCY_RATE"), lit(1)))
        )
        .withColumn(
            "INR_BALANCE",
            round(col("BALANCE") * col("APPLIED_RATE"), 2).cast(DecimalType(25, 2))
        )
    )

    balance_df = df_converted.select(
        "CGL", "CURRENCY", "BRANCH_CODE", "BALANCE", "INR_BALANCE", "BALANCE_DATE"
    )

    # ===================================================================================
    #  STEP 28: Prepare Delta Lake final DataFrame
    # ===================================================================================
    deltalake_final = balance_df.select(
        concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")).alias("glcc"),
        col("BALANCE").cast(DecimalType(25, 4)).alias("closing_balance"),
        col("INR_BALANCE").cast(DecimalType(25, 4))
    ).withColumn("BALANCE_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))

    # Materialize — used in multiple writes below
    deltalake_final.cache()
    deltalake_final.count()

    # Unpersist df_gl_balance_final — no longer needed
    df_gl_balance_final.unpersist()

    # ===================================================================================
    #  STEP 29: Write manifest file to HDFS
    # ===================================================================================
    try:
        java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(spark._jvm, "org.apache.hadoop.fs.Path")
        fs_manifest = spark._jvm.FileSystem.get(spark._jsc.hadoopConfiguration())

        hdfs_dir        = f"{BASE_MANIFEST_PATH}/{etl_date_path}"
        manifest_file   = f"Gl_balance_successful_{RUN_ID}_Manifest.txt"
        dest_file_path  = spark._jvm.Path(f"{hdfs_dir}/{manifest_file}")

        log_etl(spark, process_run_id, "gl_balance_manifest_writing", 1, "Starting manifest stage.")

        hdfs_dir_obj = spark._jvm.Path(hdfs_dir)
        if not fs_manifest.exists(hdfs_dir_obj):
            fs_manifest.mkdirs(hdfs_dir_obj)

        out_stream = fs_manifest.create(dest_file_path, True)
        out_stream.write(bytearray("GL_balance successful", "utf-8"))
        out_stream.close()

        logger.info(f"Manifest written to HDFS: {dest_file_path}")
        log_etl(spark, process_run_id, "gl_balance_manifest_writing", 2, "Manifest uploaded successfully.")

    except Exception as e:
        logger.error(f"Error in manifest creation: {e}")
        log_etl(spark, process_run_id, "gl_balance_manifest_writing", 3, "Error in creating manifest file")

    # ===================================================================================
    #  STEP 30: Write BALANCE to Oracle
    #           RUN_ID==1: DELETE existing then INSERT; else just INSERT
    #           Business logic preserved exactly
    # ===================================================================================
    try:
        if RUN_ID == 1:
            conn = None
            stmt = None
            try:
                conn = spark._jvm.java.sql.DriverManager.getConnection(
                    oracle_url, oracle_user, oracle_password
                )
                conn.setAutoCommit(False)
                delete_sql = f"""DELETE FROM {BALANCE}
                                 WHERE TRUNC(BALANCE_DATE) = TO_DATE('{posting_date_str}', 'YYYY-MM-DD')"""
                stmt = conn.createStatement()
                deleted_rows = stmt.executeUpdate(delete_sql)
                conn.commit()
                logger.info(f"Deleted {deleted_rows} rows for {posting_date_str}")
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"Error while deleting old data: {e}")
                raise
            finally:
                if stmt:
                    stmt.close()
                if conn:
                    conn.close()

        write_oracle(balance_df, BALANCE)
        logger.info(f"Successfully data written into balance table")
        log_etl(spark, process_run_id, "closing_balance_updated_oracle", 2, "Closing balance updated successfully in oracle", 0)

    except Exception as e:
        logger.info(f"Error writing BALANCE: {e}")
        log_etl(spark, process_run_id, "file_reading_started", 3, "Error saving gl_balance to oracle")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # ===================================================================================
    #  STEP 31: Write PPF Transactions to Oracle
    # ===================================================================================
    try:
        write_oracle(PPF_Posting, TRANSACTIONS)
        logger.info(f"=== Data successfully written to transaction table Oracle DB ===")
        log_etl(spark, process_run_id, "transaction_posted", 2, "Transactions posting posted successfully", 0)
    except Exception as e:
        logger.info(f"Error writing to Oracle DB: {e}")
        log_etl(spark, process_run_id, "file_reading_started", 3, "Error in posting transactions")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # Unpersist PPF_Posting — written, no longer needed
    PPF_Posting.unpersist()

    # ===================================================================================
    #  STEP 32: Write FCNB Transactions to Oracle
    # ===================================================================================
    try:
        write_oracle(FCNB_transactions, TRANSACTIONS)
        logger.info(f"=== Data successfully written to transaction table Oracle DB ===")
        log_etl(spark, process_run_id, "transaction_posted_fncb", 2, "FNCB Transactions posting posted successfully", 0)
    except Exception as e:
        logger.error(f"Error writing to Oracle DB: {e}")
        log_etl(spark, process_run_id, "file_reading_started", 3, "Error in posting fncb transactions")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # ===================================================================================
    #  STEP 33: Write INVALID records to Oracle
    # ===================================================================================
    try:
        write_oracle(invalid_df, INVALID)
        logger.info(f"=== Invalid records written to DB ===")
    except Exception as e:
        logger.info(f"Error writing INVALID records: {e}")
        log_etl(spark, process_run_id, "file_reading_started", 3, "Error in posting invalid items.")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # ===================================================================================
    #  STEP 34: Write closing balance to Delta Lake
    #           RUN_ID==1: overwrite partition; else append
    #           Business logic preserved exactly
    # ===================================================================================
    try:
        if RUN_ID == 1:
            deltalake_final.write.format("delta") \
                .mode("overwrite") \
                .option("replaceWhere", f"BALANCE_DATE = '{posting_date_str}'") \
                .save(GL_DATALAKE_PATH)
            log_etl(spark, process_run_id, "closing_balance_updated_datalake", 2, "Closing balance updated successfully in delta lake", 0)
        else:
            deltalake_final.write.format("delta") \
                .mode("append") \
                .save(GL_DATALAKE_PATH)
            log_etl(spark, process_run_id, "closing_balance_updated_datalake", 2, "Closing balance updated successfully in delta lake", 0)
    except Exception as e:
        logger.info(f"Error during saving in datalake  {e}")
        log_etl(spark, process_run_id, "file_reading_started", 3, "Error saving gl_balance to delta lake")
        log_etl(spark, process_run_id, "glif_started", 3, "Error in Glif ETL process")
        spark.stop()
        sys.exit(1)

    # Unpersist deltalake_final — job complete
    deltalake_final.unpersist()

    # ===================================================================================
    #  STEP 35: Final ETL log + Spark stop
    # ===================================================================================
    log_etl(spark, process_run_id, "file_reading_started", 2, "File reading completed.")
    log_etl(spark, process_run_id, "glif_started", 2, "Glif ETL process Ended")

    spark.stop()


if __name__ == "__main__":
    main(sys.argv)
