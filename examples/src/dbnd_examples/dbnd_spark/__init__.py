from dbnd_examples.data import dbnd_examples_src_path


def spark_folder(*path):
    return dbnd_examples_src_path("dbnd_spark", *path)
