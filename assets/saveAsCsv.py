from dagster import asset,Nothing,get_dagster_logger
import pandas as pd

logger=get_dagster_logger()

@asset()
def saveDataAsCsv(fetch_from_pg):
    """
    
    Writes the data received into a CSV file

    """

    if fetch_from_pg is not Nothing :
        df= pd.DataFrame(fetch_from_pg,columns=['job_id','status','updated_at'])
        df.to_csv("job_info.csv",index=False,header=True)
    else : 
        logger.info("NO JOBS TO EXECUTE !!")