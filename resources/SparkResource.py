from dagster import ConfigurableResource

class SparkResource(ConfigurableResource):
    """
    Reusable Spark Resource to connect to spark master
    
    """
    master_url: str = "local[*]"
    python_script: str
    executor_memory: str = "4g"
    executor_cores: str = "2"