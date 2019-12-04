from dbnd import project_path


def spark_folder(*path):
    return project_path("dbnd_examples/dbnd_spark", *path)
