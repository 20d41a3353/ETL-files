import datetime 
from pyspark.sql import SparkSession , functions as F
import pyspark.sql.types as T
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import  to_date,expr, round, lower, col,when, length, trim,count, substring,trim, concat, lit, create_map, to_timestamp, sum,broadcast, coalesce,current_timestamp, first, input_file_name,substring_index
from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType, DateType, TimestampType
from datetime import date, timedelta
from pyspark.sql.functions import desc
import sys
from py4j.java_gateway import java_import
from delta.tables import DeltaTable 
import concurrent.futures
import builtins

from pyspark.sql import Row
from pyspark.sql.functions import expr, lpad, floor, concat_ws
from pyspark.sql.functions import ltrim, col
from common.create_path_if_not_exists import create_path_if_not_exists
from common.pattern import read_patterns
import re   
from functools import reduce
from pyspark.sql.functions import col, substring, when, expr, ascii
# =========== Common Methods =============================

from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
from datetime import datetime,date
from common.read_write_oracle import read_oracle

from pyspark.sql.functions import col, trim, substring,regexp_replace
from collections import defaultdict
def main(etl_date):
    print("hello")
    # GLIFONL_12032026_h_120
    # /CBS-FILES/01-04-2026/GLIF/a/GLIFBOR_01042026_a_097.gz
    BASE_HDFS = get_hdfs_base()
    

    date_str = etl_date

    date_obj = datetime.strptime(date_str, "%d-%m-%Y")

    output_date = date_obj.strftime("%d-%b-%y").upper()

    print(output_date)

    # +++++++++++++++++++++++++++++++++++++++++++
    # +++++++++++++ PATHS +++++++++++++++++++++++ 121483603
    # +++++++++++++++++++++++++++++++++++++++++++
    output_path =  f"{BASE_HDFS}/druid-data-lake/{date_str}/GLIF/"
    input_path = f"{BASE_HDFS}/CBS-FILES/{date_str}/GLIF"


    control_file_master =f"{BASE_HDFS}/druid-data-lake/{date_str}/GLIF_CFM"

    spark = create_spark_session("Bulk Data PIPELINE", BASE_HDFS)
    query = f"""(select FILE_NAME,HDFS_ROW_COUNT from controll_file_master where ETL_DATE = '{output_date}' and FILE_TYPE ='GLIF'  ) e"""
    control_file_master_df = read_oracle(spark, query)
    # Extract the values into a list of Row objects, then pull the specific field
    my_list = [row['FILE_NAME'] for row in control_file_master_df.select('FILE_NAME').collect()]



    pattern = r'^(.*?)(\d{8})_([a-zA-Z])_.*$'

    grouped = defaultdict(list)

    for fname in my_list:
        match = re.match(pattern, fname)
        if match:
            stream = match.group(3)
            grouped[stream].append(fname)
	
    all_paths = []
    for stream, files in grouped.items():
        paths = [f"{input_path}/{stream}/{ft}.gz" for ft in files]
        all_paths.extend(paths)

    # all_paths = read_patterns(spark,all_paths)
   
    # print(f"{all_paths}")
    df_raw = spark.read.format("text").load(all_paths).withColumn("File_name", regexp_replace(substring_index(input_file_name(), "/",-1), r"\.gz$",""))
    
    file_count_df = df_raw.groupBy("FILE_NAME").agg(count("*").alias("ROW_COUNT"))

    
    from pyspark.sql.functions import col, lit

    comparison_df = (
        control_file_master_df.alias("c")
        .join(
            file_count_df.alias("f"),
            ["FILE_NAME"],
            "full_outer"
        )
        .select(
            col("FILE_NAME"),
            col("c.HDFS_ROW_COUNT"),
            col("f.ROW_COUNT"),
            (col("f.ROW_COUNT") - col("c.HDFS_ROW_COUNT")).alias("DIFFERENCE")
        )
    )

    mismatch_df = comparison_df.filter(
        (col("HDFS_ROW_COUNT").isNull()) |
        (col("ROW_COUNT").isNull()) |
        (col("HDFS_ROW_COUNT") != col("ROW_COUNT"))
    )

    create_path_if_not_exists(spark,control_file_master)
    if mismatch_df.rdd.isEmpty():

        matched = spark.createDataFrame(
            [("SUCCESS : ALL FILES ROW COUNT MATCHED",)],
            ["REPORT_LINE"]
        )
        matched.coalesce(1).write.mode("overwrite").text(control_file_master)

    else:

        mismatch_df.selectExpr(
            """
            concat(
                'FILE_NAME=', FILE_NAME,
                ' | CONTROL_COUNT=', cast(HDFS_ROW_COUNT as string),
                ' | ACTUAL_COUNT=', cast(ROW_COUNT as string),
                ' | DIFFERENCE=', cast(DIFFERENCE as string)
            ) as REPORT_LINE
            """
        ).coalesce(1).write.mode("overwrite").text(control_file_master)

        sc = spark.sparkContext
        # 1. Setup Hadoop FileSystem Access
        jvm = sc._jvm
        jsc = sc._jsc
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
        Path = jvm.org.apache.hadoop.fs.Path

        manifest_filename = f"MisMatched_Count.txt"
        dest_file_path = spark._jvm.Path(f"{control_file_master}/{manifest_filename}")

        def df_to_show_string(df,  max_rows=10000):
            count = df.count()
            table = df._jdf.showString(count, 50, False)
            return  table
        mismatch_df = df_to_show_string(mismatch_df)
        

        # 4. Ensure HDFS directory exists
        hdfs_dir_obj = spark._jvm.Path(control_file_master)
        if not fs.exists(hdfs_dir_obj):
            fs.mkdirs(hdfs_dir_obj)

 
        out_stream = fs.create(dest_file_path, True)
        out_stream.write(bytearray(mismatch_df, "utf-8"))
        out_stream.close()
        


       
       


    query_parm = f"""(select * from FILE_DATA_PARAMETERS where FILE_TYPE ='GLIF' ) e"""
    parameters = read_oracle(spark, query_parm)

    metadata = parameters.collect()
    
    transformed_df = df_raw
    columns = []
    for row in metadata:
       if row["TO_BE_INCLUDED"] == "yes":
           column_name = row["COLUMN_NAME"]
           
           columns.append(column_name)
           start_pos = int(row["START_POSITION"])
           end_pos = int(row["END_POSITION"]) -1
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
    print(columns)
    
    transformed_df =transformed_df.drop("value")    
    transformed_df = transformed_df.withColumn(
        "POST_DATE", 
        expr("date_add(to_date('1899-12-31'), cast(POST_DATE as int))")
        ).withColumn(
            "TRANS_DATE", 
            expr("date_add(to_date('1899-12-31'), cast(TRANS_DATE as int))")
            ).withColumn(
                    "POST_TIME",
                    concat(col("POST_DATE"),lit(" "),
                           expr("""
                                concat(
                                lpad(cast(floor(POST_TIME / 3600000) as string), 2, '0'),
                                ':',
                                lpad(cast(floor((POST_TIME % 3600000) / 60000) as string), 2, '0'),
                                ':',
                                lpad(cast(floor((POST_TIME % 60000) / 1000) as string), 2, '0')
                                )"""))
                                ).withColumn(
                                    "ACCOUNT",
                                     regexp_replace(col("ACCOUNT"), r"^0+", ""))
    
    # Create digit mapping once
    digit_map = F.create_map(
        *[x for pair in [
            (F.lit('0'), F.lit('0')),(F.lit('1'), F.lit('1')),(F.lit('2'), F.lit('2')),(F.lit('3'), F.lit('3')),(F.lit('4'), F.lit('4')),
            (F.lit('5'), F.lit('5')),(F.lit('6'), F.lit('6')),(F.lit('7'), F.lit('7')),(F.lit('8'), F.lit('8')),(F.lit('9'), F.lit('9')),
            (F.lit('p'), F.lit('0')),(F.lit('q'), F.lit('1')),(F.lit('r'), F.lit('2')),(F.lit('s'), F.lit('3')),(F.lit('t'), F.lit('4')),
            (F.lit('u'), F.lit('5')),(F.lit('v'), F.lit('6')),(F.lit('w'), F.lit('7')),(F.lit('x'), F.lit('8')),(F.lit('y'), F.lit('9'))
        ] for x in pair]
    )

    negative_chars = ['p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y']


    def decode_signed_amount(col_name):
        sign_char = F.substring(F.col(col_name), -1, 1)

        amount_str = F.concat(
            # F.substring(F.col(col_name), 1, F.length(F.col(col_name)) - 1),
            F.expr(f"substring({col_name}, 1, length({col_name})-1)"),
            digit_map[sign_char]
        )

        sign = F.when(sign_char.isin(negative_chars), -1).otherwise(1)

        return (
            amount_str.cast("decimal(25, 4)")/1000 * sign
        )

    amount_cols = [
        "FCY_AMT",
        "LCY_AMT",        
    ]

    transformed_df = transformed_df.select(
        
        *[
            decode_signed_amount(c).alias(c)
            if c in amount_cols
            else F.col(c)
            for c in transformed_df.columns
        ]
    )





    transformed_df = transformed_df.withColumns({
        "AMOUNT": when(
            col("FCY_CODE") != "INR",
            col("FCY_AMT")
        ).otherwise(col("LCY_AMT"))
    })



    transformed_df = transformed_df.select(*columns,"AMOUNT")
    

    transformed_df = transformed_df.withColumn("AMOUNT", (col("AMOUNT").cast("decimal(25, 4)"))).withColumn("FCY_AMT", (col("FCY_AMT").cast("decimal(25, 4)"))).withColumn("LCY_AMT", (col("LCY_AMT").cast("decimal(25, 4)")))




    path = output_path
    res = create_path_if_not_exists(spark,path)
    print(res)
    transformed_df.write.format("delta").mode("overwrite").save(path)
    



if __name__ == "__main__":
    date_format = "%d-%m-%Y"
    dates = ["12-03-2026", "12-03-2026"]   #[From_Date, To_Date]  

    start_date = datetime.strptime(dates[0], date_format)
    end_date = datetime.strptime(dates[1], date_format)
    
    generated_dates = []
   
    current_date = start_date

    while current_date <= end_date:       
        generated_dates.append(current_date.strftime(date_format))        
        current_date += timedelta(days=1)
    
    print(generated_dates)

    for etl_date in generated_dates:
        main(etl_date)
