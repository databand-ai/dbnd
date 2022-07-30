# © Copyright Databand.ai, an IBM Company 2022

import logging
import os

from os.path import exists

import pytest

from dbnd.testing.helpers import (
    get_environ_without_dbnd_and_airflow_vars,
    run_dbnd_subprocess__dbnd,
)


logger = logging.getLogger(__name__)


def run_dbnd_test_project(project_dir, args):
    env = get_environ_without_dbnd_and_airflow_vars()
    # otherwise ~/airflow folder will be used
    env["AIRFLOW_HOME"] = os.path.join(project_dir, ".airflow")
    return run_dbnd_subprocess__dbnd(args=args, env=env, cwd=project_dir)


class TestProjectMng(object):
    def test_databand_files(self, tmpdir_factory):
        # create a file "myfile" in "mydir" in temp folder
        new_project_dir = tmpdir_factory.mktemp("test_project").strpath
        output = run_dbnd_test_project(new_project_dir, "project-init")
        assert "Databand project has been initialized at" in output
        target = new_project_dir

        assert exists(target)
        assert exists(os.path.join(target, "project.cfg"))

    def test_double_init_fail(self, tmpdir_factory):
        # create a file "myfile" in "mydir" in temp folder
        new_project_dir = tmpdir_factory.mktemp("test_project").strpath
        output = run_dbnd_test_project(new_project_dir, "project-init")
        assert "Databand project has been initialized at" in output

        with pytest.raises(Exception):
            run_dbnd_test_project(new_project_dir, "project-init")

        output = run_dbnd_test_project(new_project_dir, "project-init --overwrite")
        assert "Databand project has been initialized at" in output
