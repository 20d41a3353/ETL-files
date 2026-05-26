import datetime 
from pyspark.sql import SparkSession , functions as F
import pyspark.sql.types as T

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import  to_date,expr, round, lower, col,when, length, trim,count, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce,current_timestamp, first, input_file_name
from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType, DateType, TimestampType
from datetime import date, timedelta
from pyspark.sql.functions import desc
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
from datetime import datetime,date
## from common.logdb import log_etl
#from common.log_etl_modified import log_etl
from common.PPF_FCNB import ppf_postings
from common.PPF_FCNB import monthend_posting
from common.read_write_oracle import read_oracle
from common.read_write_oracle import write_oracle
from common.provision1 import process_provision_data
from common.provision1 import reverse_provision
from common.fileUtil import get_delta_path_by_file_type
from common.processRun import get_process_run_id
from pyspark.sql.functions import col, trim, substring
from collections import defaultdict
def main():
    print("hello")
    # GLIFONL_12032026_h_120
    # /CBS-FILES/01-04-2026/GLIF/a/GLIFBOR_01042026_a_097.gz
    BASE_HDFS = get_hdfs_base()
    
    spark = create_spark_session("Bulk Data PIPELINE", BASE_HDFS)
    query = f"""(select FILE_NAME from controll_file_master where ETL_DATE = '12-MAR-26' and FILE_TYPE ='GLIF') e"""
    filenames = read_oracle(spark, query)
    filenames.show(20,False)
    # Extract the values into a list of Row objects, then pull the specific field
    my_list = [row['FILE_NAME'] for row in filenames.select('FILE_NAME').collect()]


    pattern = r'^(.*?)(\d{8})_([a-zA-Z])_.*$'

    grouped = defaultdict(list)

    for fname in my_list:
        match = re.match(pattern, fname)
        if match:
            stream = match.group(3).lower()
            grouped[stream].append(fname)

    all_paths = []
    for stream, files in grouped.items():
        # paths = [f"{BASE_HDFS}/CBS-FILES/31-01-2026/GLIF/{ft}.txt" for ft in files] 
        paths = [f"{BASE_HDFS}/CBS-FILES/12-03-2026/GLIF/{stream}/{ft}.gz" for ft in files]
        all_paths.extend(paths)

    
   
    print(f"{all_paths}")
    df_raw = spark.read.format("text").load(all_paths).withColumn("SourcePath", input_file_name()).withColumn("count",lit(1))
    
    # df_raw_count = df_raw.drop("value")
    # df_raw_count = df_raw_count.groupBy(col("SourcePath")).agg(sum("count").alias("total_count"))
    # df_raw_count.show(300,False)

    # print(f"Total no of rows ===>{df_raw.count()}")  #Total no of rows ===> 12,14,83,603

    query_parm = f"""(select * from TOTAL_DATA_PARAMETERS where FILE_TYPE ='GLIF') e"""
    parameters = read_oracle(spark, query_parm)

    metadata = parameters.collect()
    
    transformed_df = df_raw
    for row in metadata:
       if row["TOBEINCLUDED"] == "yes":
           column_name = row["COLUMNNAME"]
           start_pos = int(row["STARTVALUE"])
           end_pos = int(row["ENDVALUE"]) -1
           length = end_pos - start_pos + 1
           transformed_df = transformed_df.withColumn(
               column_name,
               trim(
                   substring(
                       col("value"),
                       start_pos,
                       length
                   )
               )
           )
    transformed_df =transformed_df.drop("value")
    # transformed_df.show(20,False)

    df_processed = transformed_df.withColumns({
        "Id": concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")),
        "currency_code": col("CURRENCY_IND")
    })

    df_clean = df_processed.withColumns({
        "Amount_raw": when(
            col("currency_code") != "INR",
            col("FOREIGN_AMOUNT")
        ).otherwise(col("AMOUNT"))
    })

    df_clean = df_clean.withColumns({
        "Amount_base": substring(col("Amount_raw"), 1, 16),
        "sign_char": substring(col("Amount_raw"), 17, 1)
    })

    digit_map = create_map(
        lit('1'), lit('1'), lit('2'), lit('2'), lit('3'), lit('3'), lit('4'), lit('4'), lit('5'), lit('5'),
        lit('6'), lit('6'), lit('7'), lit('7'), lit('8'), lit('8'), lit('9'), lit('9'), lit('0'), lit('0'),
        lit('p'), lit('0'), lit('q'), lit('1'), lit('r'), lit('2'), lit('s'), lit('3'), lit('t'), lit('4'),
        lit('u'), lit('5'), lit('v'), lit('6'), lit('w'), lit('7'), lit('x'), lit('8'), lit('y'), lit('9')
    )

    df_with_signed_amount = df_clean.withColumns({
        "last_digit": digit_map.getItem(col("sign_char")),
        "sign": when(col("sign_char").isin(['p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y']), -1).otherwise(1)
    })
    df_with_signed_amount = df_with_signed_amount.withColumn(
        "Amount_decimal_str",
        concat(col("Amount_base"), col("last_digit"))
    )
    df_final = df_with_signed_amount.withColumns({
        "Amount_final": (col("Amount_decimal_str").cast(DecimalType(25,4)) / 1000) * col("sign")
    })
    df_final = df_final.select("ACCOUNT_NUMBER",
"ACCOUNT_TYPE",
"BRANCH_CODE",
"CGL",
"CURRENCY",
"CURRENCY_IND",
"DESCRIPTION",
"FOREIGN_AMOUNT",
"JOURNAL_NUMBER",
"KEY_1",
"POST_DATE",
"TIME",
"TRANSACTION_CODE",
"TRANSACTION_DATE1",
"TRANSACTION_DATE2",
"TRANSACTION_TYPE",
"Amount_raw",
"Amount_final")
    path = "hdfs://10.0.17.76/GLIF/"
    df_final.write.format("delta").mode("overwrite").save(path)



if __name__ == "__main__":
    main()
