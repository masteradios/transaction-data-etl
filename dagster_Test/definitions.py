# definitions.py
import os
from dagster import Definitions, EnvVar, load_assets_from_modules,ScheduleDefinition,PipesSubprocessClient
import assets.fetch_from_pg as fetch_from_pg_module
from resources.EmailResourceManager import EmailResource
from resources.PostgresManager import PostgresResource

import assets.saveAsCsv as saveAsCsv
from dagster_aws.s3 import S3Resource
from resources.S3BucketManager import S3BucketManagerResource
from jobs.Job_PrepareFilesForIngestion import prepareFilesForIngestion
from jobs.Job_ProcessFiles import processFiles,sensorTofetchFileToProcess
from jobs.Job_GenerateFileReport import generateFileReport,sensorTofetchFileToGenerateReport
from resources.SparkResource import SparkResource
fetch_from_pg= load_assets_from_modules([fetch_from_pg_module,saveAsCsv])


s3_base = S3Resource(
)

s3_manager = S3BucketManagerResource(
    bucket_name="load-transactions",
    s3_base=s3_base
)

sparkResource =SparkResource(
    master_url="local[*]",
    python_script= "/home/ubuntu/dagsterProjects/dagster_Test/spark_jobs/script.py"
)

pipes_client = PipesSubprocessClient()

prepare_files_schedule = ScheduleDefinition(
    description="Runs every minute to fetch files landed in S3 rawFiles folder",
    job=prepareFilesForIngestion,
    cron_schedule="* * * * *"
)

defs = Definitions(
    jobs=[prepareFilesForIngestion,processFiles,generateFileReport],
    schedules=[prepare_files_schedule],
    sensors=[sensorTofetchFileToProcess,sensorTofetchFileToGenerateReport],
    resources={
        "postgresConn": PostgresResource(),
        "s3Manager":s3_manager,
        "pipes_client": pipes_client,
        "sparkResource":sparkResource,
        "emailResource": EmailResource(
            smtp_server=os.getenv("SMTP_SERVER", "smtp.gmail.com"),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            sender_email=os.getenv("SENDER_EMAIL"),
            sender_password=os.getenv("SENDER_PASSWORD"),
            use_tls=os.getenv("USE_TLS", "true").lower() in ("true", "1", "yes"),
            use_ssl=os.getenv("USE_SSL", "false").lower() in ("true", "1", "yes")
        )
    },
)
