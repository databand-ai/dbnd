from __future__ import absolute_import

import os

import pandas as pd

from pytest import fixture


@fixture
def simple_df():
    return pd.DataFrame(data=[[1, 1], [2, 2]], columns=["c1", "c2"])


@fixture
def pandas_data_frame():
    names = ["Bob", "Jessica", "Mary", "John", "Mel"]
    births = [968, 155, 77, 578, 973]
    df = pd.DataFrame(data=list(zip(names, births)), columns=["Names", "Births"])
    return df


@fixture
def pandas_data_frame_histograms(pandas_data_frame):
    return {
        "Births": ([2, 0, 1, 2], [77.0, 301.0, 525.0, 749.0, 973.0],),
    }


@fixture
def pandas_data_frame_stats(pandas_data_frame):
    return {
        "Births": {
            "count": 5.0,
            "mean": 550.2,
            "std": 428.42467249214303,
            "min": 77.0,
            "25%": 155.0,
            "50%": 578.0,
            "75%": 968.0,
            "max": 973.0,
            "non-null": 5,
            "null-count": 0,
            "distinct": 5,
        }
    }


@fixture
def s1_root_dir(tmpdir):
    dir_path = str(tmpdir.join("dir.csv/"))
    os.makedirs(dir_path)
    return dir_path + "/"


@fixture
def s1_file_1_csv(s1_root_dir, simple_df):
    path = os.path.join(s1_root_dir, "1.csv")
    simple_df.head(1).to_csv(path, index=False)
    return path


@fixture
def s1_file_2_csv(s1_root_dir, simple_df):
    path = os.path.join(s1_root_dir, "2.csv")
    simple_df.tail(1).to_csv(path, index=False)
    return path


@fixture
def s1_dir_with_csv(s1_root_dir, s1_file_1_csv, s1_file_2_csv):
    return s1_root_dir, s1_file_1_csv, s1_file_2_csv
