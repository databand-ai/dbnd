import mock
import pytest

from dbnd import dbnd_config, dbnd_run_cmd, relative_path, task
from dbnd._core.current import try_get_current_task_run
from dbnd._core.task_build.task_registry import build_task_from_config


def is_sub_dict(left, right):
    # is the right value a sub dict of the left
    return all(k in left and left[k] == right[k] for k in right)


@task
def pod_builder(config_name):
    # explicitly build config for k8s
    k8s_config = build_task_from_config(task_name=config_name)
    pod = k8s_config.build_pod(task_run=try_get_current_task_run(), cmds=["dummy"])
    return pod


@task
def request_builder(config_name):
    k8s_config = build_task_from_config(task_name=config_name)
    pod = k8s_config.build_pod(task_run=try_get_current_task_run(), cmds=["dummy"])
    return k8s_config.build_kube_pod_req(pod)


@pytest.fixture
def databand_context_kwargs():
    # we want extra tracking "debug" , so we can see all "tracking" calls on the screen
    return dict(conf={"core": {"tracker": ["console"]}})


K8S_CONFIG = {
    "kubernetes": {
        "limits": {"test_limits": 1},
        "requests": {"memory": "1536Mi", "cpu": "1"},
        "container_tag": "dummy_tag",
        "namespace": "test_namespace",
    }
}


class TestKubernetesPodBuild(object):
    @mock.patch("kubernetes.client.CoreV1Api")
    def test_submit_driver_req(self, mock_client):
        with dbnd_config(K8S_CONFIG):
            dbnd_run_cmd(
                [
                    "dbnd_sanity_check",
                    "--env=gcp_k8s",
                    "--set-config",
                    "kubernetes.container_tag=tag",
                ]
            )
        calls = mock_client().create_namespaced_pod.call_args_list
        assert len(calls) == 1

        call = calls[0].kwargs

        # 1) test - default labels
        req_labels = call["body"]["metadata"]["labels"]
        assert is_sub_dict(
            req_labels,
            {
                "dbnd_task_family": "d.d.d.docker-run-task",
                "dbnd_task_name": "dbnd-driver-run",
                "dbnd_task_af_id": "dbnd-driver-run",
                "dbnd": "dbnd-system-task-run",
            },
        )
        # 2) test -  running the driver with the global resources
        assert call["body"]["spec"]["containers"][0]["resources"] == {
            "limits": {"test_limits": 1},
            "requests": {"memory": "1536Mi", "cpu": "1"},
        }

    def test_pod_building(self):
        with dbnd_config(K8S_CONFIG):

            run = pod_builder.dbnd_run(config_name="gcp_k8s_engine")
            pod = run.run_executor.result.load("result")

        assert is_sub_dict(
            pod.metadata.labels,
            {
                "dbnd_task_family": "t.k.t.pod-builder",
                "dbnd_task_name": "pod-builder",
                "dbnd_task_af_id": "pod-builder",
            },
        )
        container = pod.spec.containers[0]
        assert container.resources.limits == {"test_limits": 1}
        assert container.resources.requests == {"memory": "1536Mi", "cpu": "1"}
        raw_env = {env_var.name: env_var.value for env_var in container.env}
        assert is_sub_dict(
            raw_env,
            {
                "DBND__POD_NAMESPACE": "test_namespace",
                "DBND__ENV_IMAGE": "gcr.io/dbnd-dev-260010/databand:dummy_tag",
                "DBND__GCP_K8S_ENGINE__IN_CLUSTER": "True",
                "AIRFLOW__KUBERNETES__IN_CLUSTER": "True",
            },
        )

        assert pod.metadata.namespace == "test_namespace"

    def test_custom_yaml(self):
        with dbnd_config(
            {
                "kubernetes": {
                    "pod_yaml": relative_path(__file__, "custom_pod.yaml"),
                    "container_tag": "dummy_tag",
                    "namespace": "test_namespace",
                }
            }
        ):
            run = request_builder.dbnd_run(config_name="gcp_k8s_engine")
            req = run.run_executor.result.load("result")

        spec = req["spec"]

        assert spec["dnsPolicy"] == "ClusterFirstWithHostNet"

    def test_req_building(self):
        with dbnd_config(K8S_CONFIG):
            run = request_builder.dbnd_run(config_name="gcp_k8s_engine")
            req = run.run_executor.result.load("result")
        assert is_sub_dict(
            req["metadata"]["labels"],
            {
                "dbnd_task_family": "t.k.t.request-builder",
                "dbnd_task_name": "request-builder",
                "dbnd_task_af_id": "request-builder",
            },
        )

        container_spec = req["spec"]["containers"][0]
        container_spec_env = {
            v["name"]: v["value"] for v in container_spec["env"] if "value" in v
        }
        assert is_sub_dict(
            container_spec_env,
            {
                "DBND__POD_NAMESPACE": "test_namespace",
                "DBND__ENV_IMAGE": "gcr.io/dbnd-dev-260010/databand:dummy_tag",
                "DBND__GCP_K8S_ENGINE__IN_CLUSTER": "True",
                "AIRFLOW__KUBERNETES__IN_CLUSTER": "True",
                "AIRFLOW__KUBERNETES__DAGS_IN_IMAGE": "True",
            },
        )

        assert container_spec["resources"] == {
            "requests": {"memory": "1536Mi", "cpu": "1"},
            "limits": {"test_limits": 1},
        }
        assert container_spec["image"] == "gcr.io/dbnd-dev-260010/databand:dummy_tag"
