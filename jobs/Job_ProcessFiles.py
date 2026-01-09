import base64
import json
import os
from pathlib import Path
from dagster import Config,failure_hook,success_hook, In, MetadataValue, Nothing, PipesSubprocessClient, SkipReason,get_dagster_logger,RunsFilter, DagsterRunStatus,job,sensor,op,DynamicOut,DynamicOutput,Out,Output,RunRequest,DefaultSensorStatus,SensorResult
from configs.IngestConfig import DataRowConfig
from resources.EmailResourceManager import EmailResource
from resources.PostgresManager import PostgresResource
import datetime,time

from resources.S3BucketManager import S3BucketManagerResource
from resources.SparkResource import SparkResource
logger= get_dagster_logger()

@sensor(name="sensorTofetchFileToProcess",
        job_name="processFiles",
        description="Sensor that runs every 30 seconds and triggers a run to Process Files whose status is 'RECEIVED'",
        minimum_interval_seconds=30,
        default_status=DefaultSensorStatus.STOPPED)
def sensorTofetchFileToProcess(context,postgresConn:PostgresResource):
    """
    Sensor that runs every 30 seconds and triggers a run to Process Files whose status is 'RECEIVED'
    
    """
    postgres=postgresConn.get_connection()
    queued_runs = context.instance.get_runs_count(
        filters=RunsFilter(
            job_name="processFiles",
            statuses=[DagsterRunStatus.QUEUED, DagsterRunStatus.STARTING,DagsterRunStatus.STARTED]
        )
    )
    if queued_runs < 5 : 
        
        postgres.autocommit=False
        with postgres :
            with postgres.cursor() as cur :
                    
                    cur.execute(
                        """
                        SELECT * FROM JOB_INFO 
                        WHERE STATUS ='RECEIVED'
                        ORDER BY JOB_ID DESC
                        LIMIT 5
                        FOR UPDATE SKIP LOCKED
                        """
                    )
                    rows=cur.fetchall()

                    if rows:
                        for row in rows:
                            cur.execute(
                                """
                                UPDATE JOB_INFO SET  STATUS ='PROCESSING' WHERE
                                JOB_ID = %s 
                                RETURNING *
                            """,[row[2]])

                            row=cur.fetchone()
                            yield RunRequest(
                            tags={

                                "job_id":str(row[2]),
                                "file_id": str(row[3])
                            },
                                run_key=f"{row[2]}_{int(time.time())}",
                                run_config={
                                    "ops": {
                                        "ingestFile": 
                                        {
                                            "config": 
                                            {    
                                                "status": str(row[0]),
                                                "time_updated":str(row[1]),
                                                "job_id": int(row[2]),
                                                "file_id": str(row[3]),
                                            }
                                        }
                                    }
                                }
                            )
                            postgres.commit()
                            
                    
                    else :
                        yield SkipReason("No jobs to execute !")
    else:
        yield SkipReason("5 Jobs already running. Will execute after one finishes")
        return 

# @op(
#     ins={"start": In(Nothing)},
#     out=Out(dict, metadata={
#         "custom_messages": MetadataValue.json({"preview": "Custom messages from Spark"}),
#     })
# )
@op(ins={"start": In(Nothing)},out=Out(DataRowConfig))
def ingestFile(context,config: DataRowConfig,sparkResource:SparkResource) -> DataRowConfig:
    """
    Submit pyspark script to spark master to perform validations.
    
    Creates the following files:
    1. Summary file - Contains validation statistics
    2. Valid file - Contains records that passed validation
    3. Reject file - Contains records that failed validation
    """

    logger.info(config)
    spark_home = os.environ.get("SPARK_HOME")
    spark_submit = os.path.join(spark_home, "bin", "spark-submit")
    python_exe =  r"/home/ubuntu/dagsterProjects/env/bin/python3"
    
    command = [
        spark_submit,
        "--master", sparkResource.master_url,
        "--executor-memory", sparkResource.executor_memory,
        "--executor-cores", sparkResource.executor_cores,
        "--conf", f"spark.pyspark.python={python_exe}",
        "--conf", f"spark.driver.extraJavaOptions=-DinputFile={config.file_id}",
        "--conf", f"spark.pyspark.driver.python={python_exe}",
        sparkResource.python_script
    ]
    
    result = PipesSubprocessClient().run(
        command=command,
        context=context,
        env=os.environ.copy(),
    )
    
    custom_messages = list(result.get_custom_messages())
    
    context.log.info("=" * 50)
    context.log.info("Custom Messages from Spark Job:")
    context.log.info("=" * 50)
    for i, msg in enumerate(custom_messages, 1):
        context.log.info(f"Message :")
        context.log.info(json.dumps(msg, indent=2))
    context.log.info("=" * 50)

    return DataRowConfig(
        status=config.status,
        file_id=config.file_id,
        time_updated=config.time_updated,
        job_id=config.job_id
    )

@success_hook(required_resource_keys={"postgresConn"})
def updatePostGresOnSuccess(context):
    postgres=context.resources.postgresConn.get_connection()
    postgres.autocommit=False
    run_id = context.run_id
    run = context.instance.get_run_by_id(run_id)
    tags = run.tags or {}
    logger.info(tags)
    job_id = tags.get("job_id")
    with postgres:
        with postgres.cursor()  as cur :
            cur.execute("""

             UPDATE JOB_INFO SET  STATUS ='PROCESSED' WHERE
             JOB_ID = %s 
             RETURNING *

            """,[job_id])

        postgres.commit()
        logger.info("Successfully updated Status in DB")

@op(ins={"trigger": In(DataRowConfig)} ,out=Out(Nothing))
def insertValidRowsIntoPostgres(context,trigger : DataRowConfig,postgresConn:PostgresResource,s3Manager:S3BucketManagerResource):
    """
    
    Bulk Ingest the valid files generated by Spark into Postgres
    
    """
    run_id = context.run_id
    run = context.instance.get_run_by_id(run_id)
    tags = run.tags or {}
    logger.info(tags)
    file_id = tags.get("file_id")
    local_file_path=f"/home/ubuntu/dagsterProjects/dagster_Test/validFiles/{file_id}"
    s3Manager.download_folder(s3_prefix=f"validFiles/{file_id}",local_dir=local_file_path)
    
    csv_dir = Path(local_file_path)

    latest_file = max(
        csv_dir.glob("*.csv"),
        key=lambda f: f.stat().st_mtime
    )

    if latest_file:
        logger.info(f"Processing file {latest_file}")
        
        postgres=postgresConn.get_connection()
        postgres.autocommit=False
        with postgres:
            with postgres.cursor() as cur :
                with open(latest_file, "r") as f:
                    cur.copy_expert(
                        """
                        COPY transactions (
                            id, date, client_id, card_id, amount, use_chip,
                            merchant_id, merchant_city, merchant_state, zip, mcc, errors,reject_reason
                        )
                        FROM STDIN
                        WITH (
                            FORMAT CSV,
                            HEADER TRUE,
                            QUOTE '"',
                            ESCAPE '"'
                        )
                        """,
                        f
                    )
                
            postgres.commit()



                        



@failure_hook(required_resource_keys={"postgresConn"})
def updatePostgresOnFailure(context):
    postgres=context.resources.postgresConn.get_connection()
    postgres.autocommit=False
    run_id = context.run_id
    run = context.instance.get_run_by_id(run_id)
    tags = run.tags or {}
    logger.info(tags)
    job_id = tags.get("job_id")
    with postgres:
        with postgres.cursor()  as cur :
            cur.execute("""

             UPDATE JOB_INFO SET  STATUS ='FAILED DUE TO ERROR' WHERE
             JOB_ID = %s 
             RETURNING *

            """,[job_id])
        postgres.commit()






@job(name="processFiles",hooks={updatePostgresOnFailure})
def processFiles():
    """
    Process a file that has status 'RECEIVED'

    """
    row=ingestFile()
    #a.with_hooks({slack_message_on_failure, slack_message_on_success})()
    insertValidRowsIntoPostgres.with_hooks({updatePostGresOnSuccess})(row)
    


    #Use IN and OUT to pass data from 1 op to another
