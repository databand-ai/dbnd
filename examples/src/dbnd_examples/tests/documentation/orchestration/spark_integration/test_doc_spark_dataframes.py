"""from dbnd import output, parameter
from dbnd_spark import spark, spark_task
from targets.target_config import FileFormat


#### DOC START
@spark_task(result=output.save_options(FileFormat.csv, header=True)[spark.DataFrame])
def custom_load_save_options(
    data=parameter.load_options(FileFormat.csv, header=False, sep="\t")[spark.DataFrame]
):
    print(data.show())
    return data


#### DOC END
"""
