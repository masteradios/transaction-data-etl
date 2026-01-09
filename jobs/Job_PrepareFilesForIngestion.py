from dagster import get_dagster_logger,op,job,DynamicOutput,AssetObservation,DynamicOut,Out,Output
from dagster_aws.s3 import S3Resource
from resources.S3BucketManager import S3BucketManagerResource
from resources.PostgresManager import PostgresResource
import io
import datetime

logger=get_dagster_logger()

@op(out={"inputFiles":Out(is_required=False)})
def fetchRawFilesFromS3(s3Manager: S3BucketManagerResource):
    """
    Get files from S3 Rawfiles
    
    """
    
    files= s3Manager.list_files(prefix="rawFiles/")
    inputFiles=[]
    for file in files:
        if file.endswith("/") !=True:
            file= file.removeprefix("rawFiles/")
            inputFiles.append(file)
            s3Manager.copy_file(source_folder="rawFiles",destination_folder="inputFiles",file=file)
            s3Manager.copy_file(source_folder="rawFiles",destination_folder="processedFiles",file=file,toMove=True)

    logger.info(inputFiles)
    
    if (len(inputFiles) >0) :
        yield Output(inputFiles,output_name="inputFiles")
    else :
        logger.info("No jobs to execute !")

@op
def validateMerchantId(context,postgresConn : PostgresResource,fileName:str):
    """
    Validate Each File
    
    """
    context.log.info(f"Validating: {fileName} in step")
    context.log.info(context)
    
    # record metadata that appears in the UI
    context.log_event(
        AssetObservation(asset_key="job_info_table", metadata={"processing_file": fileName})
    )

    if len(fileName) >100:
        postgres=postgresConn.get_connection()
        postgres.autocommit=False
        with postgres :
            with postgres.cursor() as cur :
                cur.execute(
                    """
                    UPDATE JOB_INFO SET  STATUS ='MERCHANT_ID INVALID' WHERE
                    FILE_ID = %s 
                """,[fileName])


@op(out={"insertedFiles": DynamicOut()})
def insertIntoPostGres(context,postgresConn: PostgresResource,files : list[str]):
    """
    Insert List of Files received from S3 into Postgres
    
    """
    postgres=postgresConn.get_connection()
    postgres.autocommit=False
    with postgres :
        with postgres.cursor() as cur :
            for file in files :

                cur.execute(
                                    """
                                    INSERT INTO JOB_INFO (status, time_updated, file_id) 
                                    VALUES ('RECEIVED', %s, %s)
                                    RETURNING *
                                    """, 
                                    (datetime.date.today(), file)
                                )
                
                insertedFileName=cur.fetchone()[3]

                logger.info(f"Inserted : {insertedFileName}")

                m_key = insertedFileName.replace(".", "_").replace("-", "_")
                yield(DynamicOutput(mapping_key=m_key,value=insertedFileName,output_name="insertedFiles"))
                postgres.commit()

            


@job(name="Prepare_Files_for_Ingestion")
def prepareFilesForIngestion():
    """
    Fetch files from S3, log them into Postgres and validate them
    
    """
    inputFiles= fetchRawFilesFromS3()
    insertedFiles=insertIntoPostGres(inputFiles)
    insertedFiles.map(validateMerchantId)
    
