# © Copyright Databand.ai, an IBM Company 2022

import os

from dbnd import dbnd_config, relative_path


TEST_CONF_FILE_NAME = "databand-test.cfg"


def _add_local_test_config(calling_file):
    local_test_file = relative_path(calling_file, TEST_CONF_FILE_NAME)
    if os.path.exists(local_test_file):
        dbnd_config.set_from_config_file(local_test_file)


def _add_global_test_config():
    curent_dir = os.path.normpath(os.getcwd())
    current_dir_split = curent_dir.split(os.sep)
    current_dir_split_reversed = reversed(list(enumerate(current_dir_split)))

    # traversing all the way up to until finding relevant anchor files
    for index, current_folder in current_dir_split_reversed:
        current_path = os.path.join("/", *current_dir_split[1 : (index + 1)])
        global_config_file = os.path.join(current_path, TEST_CONF_FILE_NAME)
        if os.path.exists(global_config_file):
            dbnd_config.set_from_config_file(global_config_file)
            break


def add_test_configuration(calling_file):
    _add_local_test_config(calling_file)
    _add_global_test_config()
