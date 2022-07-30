# © Copyright Databand.ai, an IBM Company 2022

import os

import pytest

from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase
from targets import target
from targets.dir_target import DEFAULT_FLAG_FILE_NAME
from targets.target_config import file, folder


class TestDirTarget(TargetTestBase):
    def test_read_raise_exception(self, s1_root_dir):
        t = target(s1_root_dir)

        with pytest.raises(NotImplementedError):
            with t.open("r") as fp:
                fp.read()

    def test_partitions(self, tmpdir):
        t = self.target("dir.csv/", config=file.csv)

        t_p1 = t.partition()
        assert os.path.basename(str(t_p1)) == "part-0000.csv"
        assert t._auto_partition_count == 1

        t_p2 = t.partition(name="my_partition-1")
        assert os.path.basename(str(t_p2)) == "my_partition-1.csv"

        t_p3 = t.partition(name="my_partition-2", extension=None)
        assert os.path.basename(str(t_p3)) == "my_partition-2"

    def test_read_lines_csv(self, s1_root_dir, s1_file_1_csv, s1_file_2_csv):
        expected = target(s1_file_1_csv).readlines() + target(s1_file_2_csv).readlines()

        t = target(s1_root_dir)
        actual = t.readlines()
        assert actual == expected
        assert len(actual) == 4

    def test_folder_flag(self, tmpdir):
        dir_path = str(tmpdir.join("dir.csv/"))
        os.makedirs(dir_path)
        dir_path = dir_path + os.path.sep

        t = target(dir_path)
        t_no_flag = target(dir_path, config=folder.with_flag(None))
        t_flag_default = target(dir_path, config=folder)
        t_flag = target(dir_path, config=folder.with_flag(True))

        assert not t.exists()
        assert t_no_flag.exists()
        assert not t_flag_default.exists()
        assert not t_flag.exists()

        flag = os.path.join(dir_path, DEFAULT_FLAG_FILE_NAME)
        with open(flag, "w") as fp:
            fp.write("exists")

        assert t.exists()
        assert t_flag.exists()
        assert t_flag_default.exists()

        t_flag_custom = target(dir_path, config=folder.with_flag("MY_FLAG"))
        assert not t_flag_custom.exists()

        flag = os.path.join(dir_path, "MY_FLAG")
        with open(flag, "w") as fp:
            fp.write("exists")
        assert t_flag_custom.exists()

    def test_meta_files(self, tmpdir):
        meta_files = [".a1", "a2", "a3"]
        dir_path = str(tmpdir.join("dir.csv/"))
        os.makedirs(dir_path)
        dir_path = dir_path + os.path.sep

        t_no_meta_files = target(dir_path, config=folder.without_flag())
        t_with_meta_files = target(
            dir_path, config=folder.with_meta_files(meta_files).without_flag()
        )
        t_with_meta_files_and_flag = target(
            dir_path, config=folder.with_meta_files(meta_files).with_flag(True)
        )

        assert t_no_meta_files.exists()
        assert t_with_meta_files.exists()
        assert not t_with_meta_files_and_flag.exists()

        for m in meta_files:
            meta_file = os.path.join(dir_path, m)
            with open(meta_file, "w") as fp:
                fp.write("exists")

        partition = t_no_meta_files.partition("some_partition")
        with open(partition.path, "w") as fp:
            fp.write("exists")

        assert len(t_no_meta_files.list_partitions()) == 3
        assert len(t_with_meta_files.list_partitions()) == 1

        flag = os.path.join(dir_path, DEFAULT_FLAG_FILE_NAME)
        with open(flag, "w") as fp:
            fp.write("exists")

        assert len(t_no_meta_files.list_partitions()) == 3
        assert len(t_with_meta_files.list_partitions()) == 1
        assert len(t_with_meta_files_and_flag.list_partitions()) == 1
