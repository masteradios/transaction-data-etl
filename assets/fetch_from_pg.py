from dagster import asset, ResourceParam,get_dagster_logger,Nothing

from resources.PostgresManager import PostgresResource
from psycopg2.extensions import connection

logger=get_dagster_logger()

@asset
def fetch_from_pg(postgresConn: PostgresResource):
    """
    
    Fetch latest job_id from from Postgres table : Job_info
    
    
    """
    postgres=postgresConn.get_connection()
    postgres.autocommit=False
    with postgres:
        with postgres.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM job_info
                        WHERE STATUS = 'PENDING'
                ORDER BY job_id DESC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """)
            row = cur.fetchall()

            
            if row:
                cur.execute(
                    """
                    UPDATE JOB_INFO SET  STATUS ='PROCESSING' WHERE
                    JOB_ID = %s 
                    RETURNING *
                """,[row[0][0]])

                row=cur.fetchall()

                logger.info(row)

                return row

            else :
                logger.info("No jobs to execute ! ")
                return None
    
