import base64
import time
from dagster import In, Nothing, PipesSubprocessClient, failure_hook, get_dagster_logger,op,job,sensor,asset,DynamicOut,DynamicOutput,Out,Output,SkipReason,RunRequest,DefaultSensorStatus, success_hook
from configs.MerchantConfig import MerchantRowConfig
from resources.EmailResourceManager import EmailResource
from resources.PostgresManager import PostgresResource
from configs.IngestConfig import DataRowConfig
from resources.S3BucketManager import S3BucketManagerResource

logger =get_dagster_logger()
@sensor(name="sensorTofetchFileToGenerateReport",
        job_name="generateFileReport",description="Sensor that runs every 30 seconds and triggers a run to Generate Pdf and send Emails of Files whose status is 'PROCESSED'",
        minimum_interval_seconds=30,
        default_status=DefaultSensorStatus.STOPPED)
def sensorTofetchFileToGenerateReport(context,postgresConn:PostgresResource):
    """
    Sensor that runs every 30 seconds and triggers a run to Generate Pdf and send Emails of Files whose status is 'PROCESSED'
    
    """
    postgres=postgresConn.get_connection()
    postgres.autocommit=False
    with postgres:
        with postgres.cursor() as cur :
                cur.execute(
                    """
                    SELECT * FROM JOB_INFO 
                    WHERE STATUS ='PROCESSED'
                    ORDER BY JOB_ID DESC
                    LIMIT 5
                    FOR UPDATE SKIP LOCKED
                    """
                    )
                rows=cur.fetchall()
                if rows:
                     for row in rows :
                          cur.execute(
                               """
                                UPDATE JOB_INFO SET
                                STATUS ='GENERATING REPORT'
                                WHERE JOB_ID = %s
                                RETURNING *
                               """,[row[2]]
                          )
                          insertedRow=cur.fetchone()
                          yield RunRequest(
                            tags={

                                "job_id":str(insertedRow[2]),
                                "file_id": str(insertedRow[3])
                            },
                               run_key=f"{insertedRow[2]}_{int(time.time())}",
                                run_config={
                                    "ops": {
                                        "generateReport": 
                                        {
                                            "config": 
                                            {    
                                                "status": str(insertedRow[0]),
                                                "time_updated":str(insertedRow[1]),
                                                "job_id": int(insertedRow[2]),
                                                "file_id": str(insertedRow[3]),
                                            }
                                        }
                                    }
                                })
                          postgres.commit()
                else:
                     yield SkipReason("No jobs to execute !")
                          
    


@op(ins={"merchantInfo": In(MerchantRowConfig)}, out=Out(Nothing))
def generateReport(context,config:DataRowConfig,merchantInfo:MerchantRowConfig,s3Manager:S3BucketManagerResource, pipes_client: PipesSubprocessClient):
    """
    
    Generate Report PDF using summary File
    
    """
    context.log.info(f"File {config.file_id}")
    s3Manager.download_folder(s3_prefix=f"summaryFiles/{config.file_id}",local_dir=f"/home/ubuntu/dagsterProjects/dagster_Test/summaryFiles/{config.file_id}")
    jar_path = "/home/ubuntu/dagsterProjects/pdfJar/generateReportPdf-1.0-SNAPSHOT.jar"
    vm_arg = (
        f"-DfileId={config.file_id} "
        f"-DmerchantId={merchantInfo.merchant_id} "
        f"-DmerchantName={merchantInfo.merchant_name} "
        f"-DbusinessType={merchantInfo.business_type} "
        f"-DmerchantState={merchantInfo.merchant_state}"
    )
    
    
    command = [
        "java",
        f"-DfileId={config.file_id}",
        f"-DmerchantId={merchantInfo.merchant_id}",
        f"-DmerchantName={merchantInfo.merchant_name.replace(' ', '_')}",
        f"-DbusinessType={merchantInfo.business_type.replace(' ', '_')}",
        f"-DmerchantState={merchantInfo.merchant_state}",
        "-jar",
        jar_path
    ]
    logger.info(command)
    pipes_client.run(
        command=command,
        context=context
    ).get_results()

@failure_hook(required_resource_keys={"postgresConn"})
def updatePostgresOnFailure(context):
    postgres=context.resources.postgresConn.get_connection()
    postgres.autocommit=False
    run_id = context.run_id  # grab the run id
    # fetch stored tags on the run
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


@op(out=Out(MerchantRowConfig))
def getMerchantInfoFromPg(context,postgresConn:PostgresResource):
    """
    Retrieve Merchant Info from Postgres
    
    """
    postgres=postgresConn.get_connection()
    postgres.autocommit=True

    run_id = context.run_id  # grab the run id
    # fetch stored tags on the run
    run = context.instance.get_run_by_id(run_id)
    tags = run.tags or {}
    logger.info(tags)    
    merchant_id= str(tags.get("file_id")).rsplit("_", 1)[0]
    with postgres:
        with postgres.cursor() as cur :
            cur.execute("""
                                SELECT * FROM MERCHANT_INFO WHERE MERCHANT_ID = %s

                        """,[merchant_id])
            merchant_info =cur.fetchone()

            logger.info(merchant_info)
            return MerchantRowConfig(
                merchant_id=merchant_info[0],
                merchant_name=merchant_info[1],
                business_type=merchant_info[2],
                merchant_state=merchant_info[3]
    ) 


@op(ins={"trigger": In(Nothing)}, out=Out(Nothing))
def sendEmail(context,emailResource :EmailResource):
    """
    
    Send Email with Report as attachment
    
    """
    run_id = context.run_id
    run = context.instance.get_run_by_id(run_id)
    tags = run.tags or {}
    logger.info(tags)
    file_id = str(tags.get("file_id")).removesuffix(".csv")

    with open(f'/home/ubuntu/dagsterProjects/dagster_Test/reportPdfs/{file_id}.pdf', 'rb') as f:
        file_content = f.read()
        file_base64 = base64.b64encode(file_content).decode('utf-8')
    
    result = emailResource.send(
        recipient_email="songspkaditya@gmail.com",
        subject="Data Submission Report",
        template="Please find the attached report.",
        attachments=[
            {
                'filename': f"{file_id}.pdf",
                'content': file_base64,
                'mimetype': 'application/pdf'
            }
        ]
    )
    context.log.info(f"Email result: {result}")




@success_hook(required_resource_keys={"postgresConn"})
def updatePostgresOnSuccess(context):
    postgres=context.resources.postgresConn.get_connection()
    postgres.autocommit=False
    run_id = context.run_id  # grab the run id
    # fetch stored tags on the run
    run = context.instance.get_run_by_id(run_id)
    tags = run.tags or {}
    context.log.info(tags)
    job_id = tags.get("job_id")
    with postgres:
        with postgres.cursor()  as cur :
            cur.execute("""

             UPDATE JOB_INFO SET  STATUS ='INGESTION COMPLETE' WHERE
             JOB_ID = %s 
             RETURNING *

            """,[job_id])
        postgres.commit()


@job(name="generateFileReport",hooks={updatePostgresOnFailure})
def generateFileReport():
    """
    Generate Report of a File whose Processing has completed
    
    """
    merchant_info=getMerchantInfoFromPg()
    report_result = generateReport(merchant_info)
    sendEmail.with_hooks({updatePostgresOnSuccess})(report_result)
    #sendEmail.with_hooks({updatePostgresOnSuccess})()

