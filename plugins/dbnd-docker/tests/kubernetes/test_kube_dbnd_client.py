# © Copyright Databand.ai, an IBM Company 2022

from unittest.mock import Mock, patch

import pytest

from urllib3.exceptions import HTTPError

from dbnd_docker.kubernetes.kube_dbnd_client import DbndPodCtrl


def test_stream_logs_normal_flow():
    results = []

    def print_func(log_line: str):
        results.append(log_line)

    kube_client_mock = Mock(
        read_namespaced_pod_log=Mock(side_effect=[(b"ab\n", b"cd\n")])
    )

    ctrl = create_sut(kube_client_mock)
    ctrl.stream_pod_logs(print_func=print_func)

    assert results[0] == "ab"
    assert results[1] == "cd"


def create_sut(
    kube_client_mock, max_retries_on_log_stream_failure: int = 10
) -> DbndPodCtrl:
    ctrl = DbndPodCtrl(
        "fake_pod",
        "fake_ns",
        kube_config=Mock(
            max_retries_on_log_stream_failure=max_retries_on_log_stream_failure
        ),
        kube_client=kube_client_mock,
    )
    return ctrl


@patch("time.sleep", return_value=None)
def test_stream_logs_fail_retries(fake_sleep):
    results = []

    def print_func(log_line: str):
        results.append(log_line)

    def fake_iterator(*args, **kwargs):
        raise HTTPError("Testing for error handling")

    iterator_mock = Mock(side_effect=fake_iterator)
    kube_client_mock = Mock(read_namespaced_pod_log=iterator_mock)

    ctrl = create_sut(kube_client_mock)
    stream_pod_logs_mock = Mock(side_effect=fake_iterator)
    ctrl.stream_pod_logs = stream_pod_logs_mock
    result = ctrl.stream_pod_logs_with_retries()

    assert not result


def test_stream_logs_fail_flow():
    results = []

    def print_func(log_line: str):
        results.append(log_line)

    def fake_iterator(*args, **kwargs):
        yield b"ab\n"
        raise Exception("Testing for error handling")

    iterator_mock = Mock(side_effect=fake_iterator)
    kube_client_mock = Mock(read_namespaced_pod_log=iterator_mock)

    ctrl = create_sut(kube_client_mock)

    with pytest.raises(Exception):
        ctrl.stream_pod_logs(print_func=print_func)

    assert results[0] == "ab"
