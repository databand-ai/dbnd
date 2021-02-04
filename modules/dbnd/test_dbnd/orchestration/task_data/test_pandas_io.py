import logging

from dbnd import PythonTask, data, output, parameter
from targets import target


logger = logging.getLogger(__name__)


class TestPandasTask(object):
    def test_pandas_task(self, tmpdir, pandas_data_frame):
        class PandasTask(PythonTask):
            some_param = parameter[str]
            p_input = data.target
            p_output = output.json

            def run(self):
                data = self.p_input.read_df()

                logger.warning("Data at run %s: %s", self.task_name, data)

                data.to_target(self.p_output)

        pandas_target = target(str(tmpdir.join("pandas.csv")))
        pandas_data_frame.to_target(pandas_target)

        task = PandasTask(p_input=pandas_target.path, some_param=1, task_name="first")
        task_second = PandasTask(
            p_input=task.p_output, some_param=2, task_name="second"
        )

        task_second.dbnd_run()

        assert task_second.p_output
        print(task_second.p_output.read_df())
