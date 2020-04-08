import logging

import pytest

from dbnd import Task, config, new_dbnd_context
from dbnd._core.constants import ParamValidation
from dbnd._core.errors import DatabandBuildError, DatabandError, UnknownParameterError
from dbnd._core.settings import CoreConfig
from test_dbnd.factories import CaseSensitiveParameterTask, TTask, ttask_simple


logger = logging.getLogger(__name__)


class TestTaskMetaBuild(object):
    def test_verbose_build(self):
        with new_dbnd_context(conf={"task_build": {"verbose": "True"}}):
            task = TTask(override={TTask.t_param: "test_driver"})
            assert task.t_param == "test_driver"

    def test_sign_by_task_code_build(self):
        with new_dbnd_context(
            conf={"task_build": {"sign_with_full_qualified_name": "True"}}
        ):
            task = TTask()
            assert str(TTask.__module__) in task.task_meta.task_signature_source

    def test_task_call_source_class(self):
        task = TTask()
        logger.info(task.task_meta.task_call_source)
        assert task.task_meta.task_call_source
        assert task.task_meta.task_call_source[0].filename in __file__

    def test_task_call_source_func(self):
        task = ttask_simple.task()
        logger.info("SOURCE:%s", task.task_meta.task_call_source)
        assert task.task_meta.task_call_source[0].filename in __file__

    def test_case_insensitive_parameter_building(self):
        # First run with correct case
        with config(
            {
                "CaseSensitiveParameterTask": {
                    "TParam": 2,
                    "validate_no_extra_params": ParamValidation.error,
                }
            }
        ):
            dbnd_run = CaseSensitiveParameterTask().dbnd_run()
        assert dbnd_run.task.TParam == 2
        # Second run with incorrect lower case
        with config(
            {
                "CaseSensitiveParameterTask": {
                    "tparam": 3,
                    "validate_no_extra_params": ParamValidation.error,
                }
            }
        ):
            dbnd_run = CaseSensitiveParameterTask().dbnd_run()
        assert dbnd_run.task.TParam == 3
        # Third run with incorrect upper case
        with config(
            {
                "CaseSensitiveParameterTask": {
                    "TPARAM": 4,
                    "validate_no_extra_params": ParamValidation.error,
                }
            }
        ):
            dbnd_run = CaseSensitiveParameterTask().dbnd_run()
        assert dbnd_run.task.TParam == 4

    def test_wrong_config_validation(self):
        # raise exception
        with pytest.raises(UnknownParameterError) as e:
            with config(
                {
                    "TTask": {
                        "t_parammm": 2,
                        "validate_no_extra_params": ParamValidation.error,
                    }
                }
            ):
                TTask()

        assert "Did you mean: t_param" in e.value.help_msg

        # log warning to log
        with config(
            {
                "TTask": {
                    "t_parammm": 2,
                    "validate_no_extra_params": ParamValidation.warn,
                }
            }
        ):
            TTask()
        # tried to add a capsys assert here but couldn't get it to work

        # do nothing
        with config(
            {
                "TTask": {
                    "t_parammm": 2,
                    "validate_no_extra_params": ParamValidation.disabled,
                }
            }
        ):
            TTask()

        # handle core config sections too
        with pytest.raises(
            DatabandError
        ):  # might be other extra params in the config in which case a DatabandBuildError will be raised
            with config(
                {
                    "config": {"validate_no_extra_params": ParamValidation.error},
                    "core": {"blabla": "bla"},
                }
            ):
                CoreConfig()
