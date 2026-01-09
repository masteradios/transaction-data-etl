from dagster import job, op, In, Nothing, Config, Out, PipesSubprocessClient, MetadataValue
from pydantic import Field
import os
import json

from configs import PySparkConfig





@op(
    ins={"start": In(Nothing)},
    out=Out(dict, metadata={
        "custom_messages": MetadataValue.json({"preview": "Custom messages from Spark"}),
    })
)
def run_spark_job(context, config: PySparkConfig):
    
    spark_home = os.environ.get("SPARK_HOME")
    spark_submit = os.path.join(spark_home, "bin", "spark-submit")
    python_exe = r"/home/ubuntu/dagster_Test/env/bin/python3"
    
    command = [
        spark_submit,
        "--master", config.master_url,
        "--executor-memory", config.executor_memory,
        "--executor-cores", config.executor_cores,
        "--conf", f"spark.pyspark.python={python_exe}",
        "--conf", "spark.driver.extraJavaOptions=-DinputFile=dummy_trans.csv",
        "--conf", f"spark.pyspark.driver.python={python_exe}",
        config.python_script
    ]
    
    result = PipesSubprocessClient().run(
        command=command,
        context=context,
        env=os.environ.copy(),
    )
    
    custom_messages = list(result.get_custom_messages())
    
    # Log each message nicely
    context.log.info("=" * 50)
    context.log.info("Custom Messages from Spark Job:")
    context.log.info("=" * 50)
    for i, msg in enumerate(custom_messages, 1):
        context.log.info(f"Message {i}:")
        context.log.info(json.dumps(msg, indent=2))
    context.log.info("=" * 50)
    
    # Add metadata that shows up in the UI
    from dagster import Output
    return Output(
        value={
            "status": "completed",
            "custom_messages": custom_messages,
            "total_messages": len(custom_messages)
        },
        metadata={
            "Total Messages": len(custom_messages),
            "Messages": MetadataValue.json(custom_messages),
            "Status": "✓ Completed",
            "Spark Master": config.master_url,
        }
    )


@job
def pyspark_job():
    run_spark_job()