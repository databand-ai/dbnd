from typing import List

from pyspark.sql import DataFrame

from dbnd import data_source_pipeline, extend, parameter, pipeline
from dbnd._core.task.data_source_task import data_combine
from dbnd_spark import SparkConfig, spark_task
from targets import target
from targets.multi_target import MultiTarget
from targets.target_config import FileFormat, TargetConfig


# Spark doesn't have "tsv", format, so we fake it, in case parameter will get csv we will read it as tsv
tsv_df = parameter.tsv.load_options(FileFormat.csv, delimiter="\t").save_options(
    FileFormat.csv, delimiter="\t", header=True
)[DataFrame]
parquet_df = parameter.parquet[DataFrame]


@data_source_pipeline
def existing_data(
    root_path: str,
    config_ids_A: List[int],
    config_ids_B: List[int],
    config_ids_C: List[int],
) -> (MultiTarget, MultiTarget, MultiTarget):
    base_target = target(root_path)

    def get_multi_target_from_config_ids(config_ids):
        configs = []
        # multi-target config is based on the first target in the list
        # we use csv format, as spark doesn't have explicit tsv support
        target_config = TargetConfig(folder=True, format=FileFormat.csv, flag=None)
        for config_id in config_ids:
            config = target(base_target, f"configID={config_id}", config=target_config)
            configs.append(config)
        return data_combine(configs)

    data_A = get_multi_target_from_config_ids(config_ids_A)
    data_B = get_multi_target_from_config_ids(config_ids_B)
    data_C = get_multi_target_from_config_ids(config_ids_C)
    return data_A, data_B, data_C


@spark_task(
    data_a=tsv_df,
    data_b=tsv_df,
    data_c=tsv_df,
    task_config={SparkConfig.conf: extend({"spark.default.parallelism": "2"})},
    result=parquet_df.output(name="filtered_data"),
)
def filter_data(
    data_a: DataFrame,
    data_b: DataFrame,
    data_c: DataFrame,
    extra_data: parameter[DataFrame],
    num_executors: int = 500,
) -> DataFrame:
    return data_a


@pipeline
def data_source_complicated_pipeline(root_path, extra_data):
    data = existing_data(
        root_path=root_path,
        config_ids_A=[1, 2],
        config_ids_B=[3, 4],
        config_ids_C=[5, 6],
    )
    return filter_data(data[0], data[1], data[2], extra_data=extra_data)
