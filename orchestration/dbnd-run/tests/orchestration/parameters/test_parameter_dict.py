# © Copyright Databand.ai, an IBM Company 2022

from typing import Dict

from dbnd import band, task
from dbnd.testing.orchestration_utils import TargetTestBase
from targets.types import Path


@task
def t_df(p):
    # type: (Dict)-> str
    assert "a" in p
    return "OK"


class TestParameterDataFrame(TargetTestBase):
    def test_path_to_pickle_as_dict(self):
        @band
        def t_path_to_pickle_as_dict():
            # type: () -> str
            d_obj = {"a": "OK"}
            pkl_target = self.target("file.pkl")
            pkl_target.dump(d_obj)

            return t_df(Path(str(pkl_target)))

        t_path_to_pickle_as_dict.dbnd_run()
