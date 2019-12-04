from __future__ import absolute_import

from typing import List

import pandas as pd

from dbnd import output, parameter, pipeline, task
from dbnd._core.current import get_databand_context
from dbnd._core.run.databand_run import DatabandRun, new_databand_run
from dbnd.tasks import PythonTask


class TClassTask(PythonTask):
    t_param = parameter.value("1")
    t_output = output.data

    def run(self):
        self.t_output.write("%s" % self.t_param)


@task
def ttask_dataframe_generic(tparam=1, t_list=None):
    # type:(int, List[str])->pd.DataFrame
    return pd.DataFrame(data=[[tparam, tparam]], columns=["c1", "c2"])


@pipeline
def pipeline_with_generics(selected_partners):
    # type: (List[int])-> List[pd.DataFrame]
    partner_data = [
        ttask_dataframe_generic(tparam=partner, t_list=["a", "b"])
        for partner in selected_partners
    ]
    return partner_data


class TestDagrunDump(object):
    def _assert_dump(self, task):
        with new_databand_run(
            context=get_databand_context(), task_or_task_name=task
        ) as r:  # type: DatabandRun
            r.save_run()
            dump_file = r.driver_dump

        databand_run = DatabandRun.load_run(dump_file=dump_file)
        assert databand_run

    def test_dump_dagrun_simple(self, af_session):
        s = TClassTask()
        self._assert_dump(s)

    def test_dump_dagrun_generics1(self, af_session):
        s = pipeline_with_generics.task([1, 2])
        self._assert_dump(s)
