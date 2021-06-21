import gzip
import json
import logging
import os
import pickle

from typing import List

import pandas as pd
import six

from dbnd import PythonTask, config, dbnd_run_cmd, output, parameter, task
from dbnd._core.constants import OutputMode
from dbnd._core.current import get_databand_context
from dbnd.testing.helpers_pytest import assert_run_task, skip_on_windows
from dbnd_test_scenarios.test_common.task.factories import TTask
from targets import Target
from targets.target_config import file, folder


logger = logging.getLogger(__name__)


class TTaskDataOutput(PythonTask):
    simple_output = output.data
    folder_output = output.folder[Target]
    folder_data_output = output.folder_data[Target]
    data_output = output[Target]
    json_output = output.json[Target]
    pgz_output = output.pickle.gz[Target]

    def run(self):
        self.simple_output.write("test")
        self.folder_output.write("test")
        self.folder_data_output.write("test")
        self.data_output.write("test")
        self.json_output.as_object.write_json({"1": 2})
        self.pgz_output.as_object.write_pickle(tuple([1, 2, 3]))


class TProdImmutbaleOutputs(PythonTask):
    splits = output.folder(output_mode=OutputMode.prod_immutable).target

    def run(self):
        self.splits.write("a")


@task(result=output.hdf5)
def t_f_hdf5(i=1):
    # type:(int)->pd.DataFrame
    return pd.DataFrame(
        data=list(zip(["Bob", "Jessica"], [968, 155])), columns=["Names", "Births"]
    )


class TestTaskDataOutputs(object):
    def test_data_output_task(self):
        task = TTaskDataOutput()

        assert folder.csv == task.folder_output.config
        assert task.folder_output.path
        # TODO: fix to os.path.sep
        assert str(task.folder_output)[-1] == "/"

        assert file.csv == task.simple_output.config

        assert os.path.splitext(str(task.json_output))[1] == ".json"
        assert file.csv == task.data_output.config
        assert file.json == task.json_output.config

    @skip_on_windows
    def test_data_values_unix_only(self):
        t = assert_run_task(TTaskDataOutput())
        assert t.simple_output.read() == "test"
        assert t.data_output.read() == "test"
        assert json.loads(t.json_output.read()) == {"1": 2}
        assert t.pgz_output.as_object.read_pickle() == tuple([1, 2, 3])
        with gzip.open(str(t.pgz_output), "r") as fp:
            assert pickle.load(fp) == tuple([1, 2, 3])

    def test_output_override_class(self):
        class TTaskOutputOverride(PythonTask):
            simple_output = output.data

            def run(self):
                self.simple_output = "test"

        result = dbnd_run_cmd(
            [
                "TTaskOutputOverride",
                "--set",
                "TTaskOutputOverride.simple_output__target=t1.txt",
            ]
        )

        assert str(result.task.simple_output).endswith("txt")

    def test_output_override_func(self, tmpdir):
        @task
        def ttask_output_override():
            # type: ()->str
            return "test"

        override = str(tmpdir.join("output.pickle"))
        ttask_output_override.dbnd_run(result=override)
        assert os.path.exists(override)

    def test_prod_immutable_output_dict_dev(self):
        t = assert_run_task(TProdImmutbaleOutputs())
        print(t)

    def test_prod_immutable_output_same_in_dev(self):
        task = TProdImmutbaleOutputs()
        assert task.task_signature[:5] in str(task.splits)

    def test_prod_immutable_output_dict_prod(self):
        env = get_databand_context().env
        prod_env = env.clone(production=True)
        task = TProdImmutbaleOutputs(task_env=prod_env)
        assert task.task_enabled_in_prod
        assert task.task_signature[:5] not in str(task.splits)
        config.log_current_config()
        actual = assert_run_task(task)

        print(actual)

    def test_generated_output_dict(self):
        def _get_all_splits(task, task_output):  # type: (Task, ParameterBase) -> dict
            result = {}
            target = task_output.build_target(task)
            for i in range(task.parts):
                name = "part_%s" % i
                result[name] = (
                    target.partition(name="train_%s" % name),
                    target.partition(name="test_%s" % name),
                )

            return result

        class TGeneratedOutputs(PythonTask):
            parts = parameter.value(3)
            splits = output.csv.folder(output_factory=_get_all_splits)

            def run(self):
                for key, split in self.splits.items():
                    train, test = split
                    train.write(key)
                    test.write(key)

        assert_run_task(TGeneratedOutputs())

    def test_custom_parition(self):
        class CustomOutputsTTask(TTask):
            _conf__base_output_path_fmt = (
                "{root}/{env_label}/{task_family}{task_class_version}_custom/"
                "{output_name}{output_ext}/date={task_target_date}"
            )

        task = CustomOutputsTTask()
        assert_run_task(task)
        assert "CustomOutputsTTask_custom/t_output.csv/" in str(task.t_output)

    def test_multiple_outputs_inline(self):
        def _partitions(task, to):
            root = to.build_target(task)
            return {t: root.partition(name="t_output/%s" % t) for t in task.t_types}

        class TTaskMultipleOutputs(TTask):
            t_types = parameter(default=[1, 2])[List]
            t_output = output.folder(output_factory=_partitions)

            def run(self):
                for t_name, t_target in six.iteritems(self.t_output):
                    t_target.write("%s" % t_name)

        task = TTaskMultipleOutputs()
        assert_run_task(task)
        logger.error(task.t_output)

    def test_multiple_outputs_band(self):
        class TTaskMultipleOutputs(TTask):
            t_types = parameter.value([1, 2])
            t_output = output.folder()

            def band(self):
                root = self.__class__.t_output.build_output(self)
                self.t_output = {
                    t: root.partition(name="t_output/%s" % t) for t in self.t_types
                }

            def run(self):
                for t_name, t_target in six.iteritems(self.t_output):
                    t_target.write("%s" % t_name)

        task = TTaskMultipleOutputs()
        assert_run_task(task)
        logger.error(task.t_output)

    def test_hdf5_output(self):
        t_f_hdf5.dbnd_run()
