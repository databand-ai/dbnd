# © Copyright Databand.ai, an IBM Company 2022

from dbnd_examples_orchestration.data import dbnd_examples_src_path


def spark_folder(*path):
    return dbnd_examples_src_path("dbnd_spark", *path)
