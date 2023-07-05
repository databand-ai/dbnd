# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.utils.basics.path_utils import relative_path


def get_dbnd_run_conf_file(*path):
    return relative_path(__file__, "..", "conf", *path)
