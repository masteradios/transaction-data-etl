from dagster_pipes import (
    open_dagster_pipes, 
    PipesDefaultContextLoader, 
    PipesEnvVarParamsLoader, 
    PipesDefaultMessageWriter
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,to_timestamp,col,sum,regexp_replace,explode,when,count 
from pyspark.sql.types import MapType,StringType,IntegerType
import json
import pyspark.sql.functions as F


# def main():
#     with open_dagster_pipes(
#         context_loader=PipesDefaultContextLoader(),
#         params_loader=PipesEnvVarParamsLoader(),
#         message_writer=PipesDefaultMessageWriter(),
#     ) as pipes:
        
#         pipes.log.info("Starting PySpark job...")
        
#         pipes.log.info(f"DataFrame created with {df.count()} rows")
        
#         # Get head (first 3 rows)
#         head_rows = df.head(3)
        
#         # Convert to dict format for JSON serialization
#         head_data = []
#         for row in head_rows:
#             head_data.append(row.asDict())
        
#         pipes.log.info("Sending DataFrame head to Dagster...")
        
#         # Send as custom message
#         pipes.report_custom_message({
#             "dataframe_head": head_data,
#             "total_rows": df.count(),
#             "columns": df.columns
#         })
        
#         pipes.log.info("PySpark job completed!")
        
#         spark.stop()







def main(inputFile,spark):
    with open_dagster_pipes(
        context_loader=PipesDefaultContextLoader(),
        params_loader=PipesEnvVarParamsLoader(),
        message_writer=PipesDefaultMessageWriter(),
    ) as pipes:
        
        pipes.log.info("Starting PySpark job...")
        from pyspark.sql.functions import to_date,to_timestamp,col,from_json,explode,when,count
        from pyspark.sql.types import MapType,StringType,IntegerType
        s3_path = "s3a://load-transactions/inputFiles/"
        transactions_df=spark.read.csv(s3_path+inputFile,header=True)
        totalCount=transactions_df.count()
        pipes.log.info(f"Number of records : {totalCount}")
        
        transactions_df = transactions_df.withColumn('amount', regexp_replace('amount', '\\$', '')).withColumn('zip', regexp_replace('zip' ,r"\..*", ""))
        transactions_df=transactions_df.withColumn("amount", col("amount").cast("double"))



        ### Checks :
        """
        1. `Amount` not null positive
        2. `client_id` not null
        3. `date` not null and correct format
        4. `card_id` not null

        If any of the above checks fail, record is rejected
        """

        from pyspark.sql.functions import col,when,lit

        date_regex = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"

        transactions_df = transactions_df.withColumn("reject_reason", lit("-"))

        # 1.
        transactions_df = transactions_df.withColumn(
            "reject_reason",
            when(
                (col("reject_reason") == "-") &
                ((col("amount").isNull()) | ((col("amount") < 0) & (col("errors").isNull() | (col("errors") != "Insufficient Balance")))),
                "Rejected due to Invalid Amount"
            ).otherwise(col("reject_reason"))
        )

        # 2. 

        transactions_df = transactions_df.withColumn(
            "reject_reason",
            when(
                (col("reject_reason") == "-") & col("client_id").isNull(),
                "Rejected due to Invalid client_id"
            ).otherwise(col("reject_reason"))
        )

        # 3.
        transactions_df = transactions_df.withColumn(
            "reject_reason",
            when(
                (col("reject_reason") == "-") &
                ((col("date").isNull()) | (~col("date").rlike(date_regex))),
                "Rejected due to Invalid Date"
            ).otherwise(col("reject_reason"))
        )

        # 4.
        transactions_df = transactions_df.withColumn(
            "reject_reason",
            when(
                (col("reject_reason") == "-") & col("card_id").isNull(),
                "Rejected due to Invalid card_id"
            ).otherwise(col("reject_reason"))
        )


        accepted_records=transactions_df.filter(col("reject_reason")=="-")
        rejected_records=transactions_df.filter(col("reject_reason")!="-")
    

        acceptedCount=accepted_records.count()
        pipes.log.info(f"Number of accepted records : {acceptedCount}")
        
        rejectedCount=rejected_records.count()
        pipes.log.info(f"Number of rejected records : {rejectedCount}")


        summaryLine_df = (
                        transactions_df
                        .select(
                            lit("Records").alias("label"),
                            F.sum(when(col("reject_reason") == "-", 1).otherwise(0)).alias("accepted"),
                            F.sum(when(col("reject_reason") != "-", 1).otherwise(0)).alias("rejected")
                        )
                    )
        
        summary_df = (
                        transactions_df
                        .filter(col("reject_reason") != "-")
                        .groupBy("reject_reason")
                        .agg(
                            count("*").alias("total"),
                            count("reject_reason").alias("count")
                        )
                    )
        
        summary_chip_df = (
    accepted_records
    .select(
        lit("Chip").alias("label"),
        count(when(col("use_chip").isNotNull(), 1)).alias("valid_count"),
        count(when(col("use_chip").isNull(), 1)).alias("invalid_count")
    )
)

        summary_zip_df = (
            accepted_records
            .select(
                lit("ZipCode").alias("label"),
                count(when((col("zip").isNotNull()) & (col("zip").rlike("^[0-9]{5}$")), 1))
                    .alias("valid_count"),
                count(when((col("zip").isNull()) | (~col("zip").rlike("^[0-9]{5}$")), 1))
                    .alias("invalid_count")
            )
        )

        summary_state_df = (
            accepted_records
            .select(
                lit("Merchant State").alias("label"),
                count(
                    when(
                        (col("merchant_state").isNotNull()) |
                        ((col("merchant_city") == "ONLINE") & col("merchant_state").isNull()),
                        1
                    )
                ).alias("valid_count"),
                count(
                    when(
                        (col("merchant_city") != "ONLINE") & col("merchant_state").isNull(),
                        1
                    )
                ).alias("invalid_count")
            )
        )

        summary_2_df = summary_chip_df.unionByName(summary_zip_df).unionByName(summary_state_df)

        summaryLine_normalized = summaryLine_df.select(
                col("label"),
                col("accepted").alias("valid_count"),
                col("rejected").alias("invalid_count")
            )

        summary_df_normalized = summary_df.select(
            col("reject_reason").alias("label"),
            col("count").alias("valid_count"),
            lit(None).cast("long").alias("invalid_count")
        )

        final_summary = (
            summaryLine_normalized
            .unionByName(summary_df_normalized)
            .unionByName(summary_2_df)
        )




        # pipes.report_custom_message({
        #     "Number of records ": totalCount,
        #     "Number of accepted records :": acceptedCount,
        #     "Number of rejected records :": rejectedCount
        # })

        accepted_records.coalesce(1).write.mode("overwrite").csv("s3a://load-transactions/validFiles/"+inputFile,header=True)
        rejected_records.coalesce(1).write.mode("overwrite").csv("s3a://load-transactions/rejectFiles/"+inputFile,header=True)
        final_summary.coalesce(1).write.mode("overwrite").csv("s3a://load-transactions/summaryFiles/"+inputFile,header=True)


if __name__=="__main__":


    try:
        spark = (SparkSession
        .builder
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .getOrCreate())
        spark.sparkContext.setLogLevel("INFO")

        inputFile = spark._jvm.java.lang.System.getProperty("inputFile")
        main(inputFile,spark=spark)

    except Exception as e:

        print(e)
        import sys
        sys.stderr.write("Job failed, see spark_app_errors.log\n")
        
        # Exit nonzero to signal failure
        exit(1)

    finally:
        try:
            spark.stop()
        except:
            pass




#How to run 
"""spark-submit \
  --conf "spark.driver.extraJavaOptions=-DinputFile=transactions_data.csv" \
  validate.py
  """



# from pyspark.sql import SparkSession
# spark = SparkSession.builder \
#     .appName("S3DataProcessing") \
#     .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider") \
#     .getOrCreate()

# # Read from S3
# s3_path = "s3a://load-transactions/inputFiles/taxi_zone_lookup.csv"

# df = spark.read.csv(s3_path, header=True, inferSchema=True)
# print(f"Total records {df.count()}")



