# © Copyright Databand.ai, an IBM Company 2022

from dbnd import relative_path


def spark_script(*path):
    return relative_path(__file__, *path)
