# © Copyright Databand.ai, an IBM Company 2022

import pandas as pd
import pytest

from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.orchestration.features import (
    example_dict_of_data_frames,
    example_folder,
    example_output_per_id,
)
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase
from targets.dir_target import DirTarget


@pytest.fixture
def sample():
    return pd.DataFrame(data=[[1, 1], [2, 2]], columns=["c1", "c2"])


class TestFeatureOutputsExamples(TargetTestBase):
    def test_dict_of_data(self, sample):
        task = example_dict_of_data_frames.create_multiple_data_frames.task(sample)
        task = assert_run_task(task)
        assert task
        assert isinstance(task.result, DirTarget)
        assert len(task.result.list_partitions()) == 3

    def test_tutorial_partners_report(self, sample):
        task = example_folder.plot_graphs.task(data=sample)
        task = assert_run_task(task)
        assert task

    def test_tutorial_output_per_id(self, sample):
        task = example_output_per_id.output_per_id_report.task(partners=[1, 2])
        task = assert_run_task(task)
        assert task

    def test_folder_status(self):
        dir = self.target("dir.csv/")
        dir.mark_success()
        task = example_folder.read_folder_status.task(folder_input=dir.path)
        task = assert_run_task(task)
        assert task

    def test_folder_status_fails(self):
        dir = self.target("dir.csv/")
        with pytest.raises(Exception):
            task = example_folder.read_folder_status.task(folder_input=dir.path)
            assert_run_task(task)
