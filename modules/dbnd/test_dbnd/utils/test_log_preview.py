# © Copyright Databand.ai, an IBM Company 2022

import io
import os

from contextlib import contextmanager

import pytest

from mock import patch

from dbnd._core.task_run.log_preview import merge_read_log_files, read_head_and_tail


TARGET_OPEN = "io.open"

DBND_LOG_FILE = """[2020-01-01 00:00:01,000] first log line
[2020-01-01 00:00:11,000] second log line
[2020-01-01 00:00:20,000] keep with logs
[2020-01-01 00:00:35,000] more and more and more
[2020-01-01 00:00:38,000] 0,1,1,2,3,5,8,13,21,34,55
[2020-01-01 00:00:50,000] do you even Fibonacci?"""

SPARK_LOG_FILE = """[2020-01-01 00:00:13,000] sure sure
[2020-01-01 00:00:15,000] cool log line man
[2020-01-01 00:00:24,000] make sense?
[2020-01-01 00:00:29,000] empty!!
[2020-01-01 00:00:30,000] damn
[2020-01-01 00:00:31,000] damn
[2020-01-01 00:00:32,000] damn
[2020-01-01 00:00:33,000] damn
[2020-01-01 00:00:59,000] make sense!"""

FILES = {"dbnd_log": DBND_LOG_FILE, "spark_log": SPARK_LOG_FILE}


def size(path):
    return len(FILES[path])


@contextmanager
def open_mock(path, *args, **kwargs):
    yield io.BytesIO(FILES[path].encode("utf-8"))


@pytest.mark.parametrize(
    "path, head_size, tail_size, expected",
    [
        (
            "dbnd_log",  # path
            0,  # head size
            150,  # tail size
            # expected
            [
                [],
                [
                    "[2020-01-01 00:00:35,000] more and more and more",
                    "[2020-01-01 00:00:38,000] 0,1,1,2,3,5,8,13,21,34,55",
                    "[2020-01-01 00:00:50,000] do you even Fibonacci?",
                ],
            ],
        ),
        (
            "dbnd_log",  # path
            150,  # head size
            0,  # tail size
            # expected
            [
                [
                    "[2020-01-01 00:00:01,000] first log line",
                    "[2020-01-01 00:00:11,000] second log line",
                    "[2020-01-01 00:00:20,000] keep with logs",
                ],
                [],
            ],
        ),
        (
            "dbnd_log",  # path
            150,  # head size
            20,  # tail size
            # expected
            [
                # 4 lines of head
                [
                    "[2020-01-01 00:00:01,000] first log line",
                    "[2020-01-01 00:00:11,000] second log line",
                    "[2020-01-01 00:00:20,000] keep with logs",
                ],
                [],
            ],
        ),
        (
            "dbnd_log",  # path
            80,  # head size
            110,  # tail size
            [
                # two line of head
                ["[2020-01-01 00:00:01,000] first log line"],
                # two lines of tail
                [
                    "[2020-01-01 00:00:38,000] 0,1,1,2,3,5,8,13,21,34,55",
                    "[2020-01-01 00:00:50,000] do you even Fibonacci?",
                ],
            ],
        ),
        # read the whole file
        (
            "dbnd_log",  # path
            200,  # head size
            200,  # tail size
            [
                # the whole file
                [
                    "[2020-01-01 00:00:01,000] first log line",
                    "[2020-01-01 00:00:11,000] second log line",
                    "[2020-01-01 00:00:20,000] keep with logs",
                    "[2020-01-01 00:00:35,000] more and more and more",
                    "[2020-01-01 00:00:38,000] 0,1,1,2,3,5,8,13,21,34,55",
                    "[2020-01-01 00:00:50,000] do you even Fibonacci?",
                ]
            ],
        ),
    ],
)
@patch(TARGET_OPEN, new=open_mock)
def test_file_head_and_tail(monkeypatch, path, head_size, tail_size, expected):
    monkeypatch.setattr(os.path, "getsize", size)
    assert expected == list(read_head_and_tail(path, head_size, tail_size))


@pytest.mark.parametrize(
    "head_size, tail_size, expected",
    [
        # two line head for each and 2 tail for spark and one tail for dbnd
        (
            90,
            90,
            [
                [
                    "[2020-01-01 00:00:01,000] first log line",
                    "[2020-01-01 00:00:11,000] second log line",
                    "[2020-01-01 00:00:13,000] sure sure",
                    "[2020-01-01 00:00:15,000] cool log line man",
                ],
                [
                    "[2020-01-01 00:00:33,000] damn",
                    "[2020-01-01 00:00:50,000] do you even Fibonacci?",
                    "[2020-01-01 00:00:59,000] make sense!",
                ],
            ],
        ),
        #
        (
            90,
            0,
            [
                [
                    "[2020-01-01 00:00:01,000] first log line",
                    "[2020-01-01 00:00:11,000] second log line",
                    "[2020-01-01 00:00:13,000] sure sure",
                    "[2020-01-01 00:00:15,000] cool log line man",
                ],
                [],
            ],
        ),
        # merge all
        (
            1000,
            0,
            [
                [
                    "[2020-01-01 00:00:01,000] first log line",
                    "[2020-01-01 00:00:11,000] second log line",
                    "[2020-01-01 00:00:13,000] sure sure",
                    "[2020-01-01 00:00:15,000] cool log line man",
                    "[2020-01-01 00:00:20,000] keep with logs",
                    "[2020-01-01 00:00:24,000] make sense?",
                    "[2020-01-01 00:00:29,000] empty!!",
                    "[2020-01-01 00:00:30,000] damn",
                    "[2020-01-01 00:00:31,000] damn",
                    "[2020-01-01 00:00:32,000] damn",
                    "[2020-01-01 00:00:33,000] damn",
                    "[2020-01-01 00:00:35,000] more and more and more",
                    "[2020-01-01 00:00:38,000] 0,1,1,2,3,5,8,13,21,34,55",
                    "[2020-01-01 00:00:50,000] do you even Fibonacci?",
                    "[2020-01-01 00:00:59,000] make sense!",
                ]
            ],
        ),
    ],
)
@patch(TARGET_OPEN, new=open_mock)
def test_merge_read_log_files(monkeypatch, head_size, tail_size, expected):
    monkeypatch.setattr(os.path, "getsize", size)
    assert expected == merge_read_log_files(
        "dbnd_log", "spark_log", head_size, tail_size
    )
