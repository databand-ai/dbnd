# © Copyright Databand.ai, an IBM Company 2022

import os

from dbnd._core.utils.project.project_fs import abs_join


def _dbnd_examples_project_root():
    # if env var exists - use it as the examples dir, otherwise, calculate relative from here.
    dbnd_examples_project = os.getenv("DBND_EXAMPLES_PATH", None)
    if dbnd_examples_project:
        return dbnd_examples_project

    return abs_join(__file__, "..", "..", "..")


def dbnd_examples_project_path(*path):
    return os.path.join(_dbnd_examples_project_root(), *path)


def dbnd_examples_src_path(*path):
    return dbnd_examples_project_path("src", "dbnd_examples", *path)


def dbnd_examples_data_path(*path):
    return dbnd_examples_project_path("data", *path)


class ExamplesData(object):
    vegetables = dbnd_examples_data_path("vegetables_for_greek_salad.txt")
    wines = dbnd_examples_data_path("wine_quality_minimized.csv")


data_repo = ExamplesData()
