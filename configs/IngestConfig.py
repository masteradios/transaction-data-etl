from dagster import Config
class DataRowConfig(Config):
    status : str
    time_updated : str
    job_id : int
    file_id : str