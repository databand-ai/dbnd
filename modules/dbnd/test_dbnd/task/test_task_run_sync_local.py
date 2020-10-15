import logging
import os
import shutil

import pandas
import pytest

from mock import call, patch

from dbnd import task
from dbnd._core.parameter.parameter_builder import parameter
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase
from targets import LocalFileSystem, target
from targets.multi_target import MultiTarget
from targets.target_config import TargetConfig


logger = logging.getLogger(__name__)


class PseudoLocalFileSystem(LocalFileSystem):
    name = "pseudo_local"
    local = False

    def download(self, path, location, **kwargs):
        shutil.copy(path, location)

    def copy_from_local(self, local, dest, **kwargs):
        shutil.copy(local, dest)


class TestTaskRunSyncLocal(TargetTestBase):
    @pytest.fixture
    def my_target(self, pandas_data_frame):
        _target = self.target(
            "file.parquet", fs=PseudoLocalFileSystem(), config=TargetConfig(),
        )
        _target.write_df(pandas_data_frame)
        return _target

    @pytest.fixture
    def my_second_target(self, pandas_data_frame):
        _target = self.target(
            "other_file.parquet", fs=PseudoLocalFileSystem(), config=TargetConfig()
        )
        _target.write_df(pandas_data_frame)
        return _target

    @pytest.fixture
    def my_multitarget(self, my_target, my_second_target):
        return MultiTarget([my_target, my_second_target])

    @pytest.fixture
    def test_task(self, my_target):
        @task
        def t_f(input_=parameter.require_local_access()[pandas.DataFrame]):
            return input_

        return t_f

    @pytest.fixture
    def create_local_multitarget(self, my_multitarget):
        def _local_multitarget(cache_folder):
            return MultiTarget(
                [
                    target(
                        cache_folder,
                        os.path.basename(subtarget.path),
                        config=subtarget.config,
                    )
                    for subtarget in my_multitarget.targets
                ]
            )

        return _local_multitarget

    @pytest.fixture
    def mock_fs_download(self):
        return patch.object(PseudoLocalFileSystem, "download")

    @pytest.fixture
    def mock_file_metadata_registry(self):
        return patch(
            "dbnd._core.task_run.task_run_sync_local.DbndLocalFileMetadataRegistry"
        )

    def test_task_run_sync_local_multi_target(
        self,
        my_multitarget,
        test_task,
        create_local_multitarget,
        mock_fs_download,
        mock_file_metadata_registry,
    ):
        test_task = test_task.t(my_multitarget)
        task_run = test_task.dbnd_run().root_task_run
        sync_local = task_run.sync_local

        assert len(sync_local.inputs_to_sync) == 1
        task_param, old_multitarget, new_multitarget = sync_local.inputs_to_sync[0]

        assert task_param == test_task._params.get_param("input_")
        assert old_multitarget == my_multitarget

        local_multitarget = create_local_multitarget(task_run.attemp_folder_local_cache)

        self.compare_multitargets(new_multitarget, local_multitarget)

        with mock_fs_download as mocked_fs_download, mock_file_metadata_registry:
            # only pre_execute is checked because post_execute code is unreachable for MultiTargets
            sync_local.sync_pre_execute()
            mocked_fs_download.call_count == 2
            mocked_fs_download.assert_has_calls(
                [
                    call(
                        remote_subtarget.path,
                        local_subtarget.path,
                        overwrite=local_subtarget.config.overwrite_target,
                    )
                    for remote_subtarget, local_subtarget in zip(
                        my_multitarget.targets, local_multitarget.targets
                    )
                ]
            )
        # check if test_task.input_ was changed to local after sync_pre_execute
        self.compare_multitargets(test_task.input_, local_multitarget)

        sync_local.sync_post_execute()
        # check if test_task.input_ was set back to original target
        self.compare_multitargets(test_task.input_, my_multitarget)

    def test_task_run_sync_local_file_target(
        self, test_task, my_target, mock_fs_download, mock_file_metadata_registry
    ):
        test_task = test_task.t(my_target)
        task_run = test_task.dbnd_run().root_task_run
        sync_local = task_run.sync_local

        assert len(sync_local.inputs_to_sync) == 1

        task_param, old_target, new_target = sync_local.inputs_to_sync[0]

        assert task_param == test_task._params.get_param("input_")
        assert old_target == my_target
        local_target = target(
            task_run.attemp_folder_local_cache,
            os.path.basename(my_target.path),
            config=my_target.config,
        )
        assert new_target == local_target
        with mock_fs_download as mocked_fs_download, mock_file_metadata_registry:
            sync_local.sync_pre_execute()
            mocked_fs_download.assert_called_once_with(
                my_target.path,
                local_target.path,
                overwrite=local_target.config.overwrite_target,
            )
        assert test_task.input_ == local_target

        sync_local.sync_post_execute()

        assert test_task.input_ == my_target

    @staticmethod
    def compare_multitargets(multitarget, other_multitarget):
        for subtarget, other_subtarget in zip(
            multitarget.targets, other_multitarget.targets
        ):
            assert subtarget.path == other_subtarget.path
