import kubernetes.client.models as k8s

from dbnd_docker.kubernetes.vendorized_airflow.dbnd_extended_resources import (
    DbndExtendedResources,
)


def test_dbnd_extended_resources_requests():
    actual = run_now(request_memory="128M", request_cpu="0.5")
    assert actual.requests["cpu"] == "0.5"
    assert actual.requests["memory"] == "128M"


def test_dbnd_extended_resources_limits():
    actual = run_now(limit_memory="512M", limit_cpu="2.5")
    assert actual.limits["memory"] == "512M"
    assert actual.limits["cpu"] == "2.5"


def test_dbnd_extended_resources_raw_requests():
    actual = run_now(requests={"cpu": 3, "memory": "64M"})
    assert actual.requests["memory"] == "64M"
    assert actual.requests["cpu"] == 3


def test_dbnd_extended_resources_raw_limits_mixed():
    actual = run_now(limits={"nvidia.com/gpu": 1}, limit_cpu=0.5)
    assert len(actual.limits) == 2
    assert actual.limits["nvidia.com/gpu"] == 1
    assert actual.limits["cpu"] == 0.5


def test_dbnd_extended_resources_raw_requests_override():
    actual = run_now(
        requests={"cpu": "should be overridden by request_cpu"}, request_cpu="0.5"
    )
    assert actual.requests["cpu"] == "0.5"


def run_now(**kwargs) -> k8s.V1ResourceRequirements:
    sut = DbndExtendedResources(**kwargs)
    return sut.to_k8s_client_obj()
