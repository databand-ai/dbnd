from __future__ import absolute_import

import logging

import pytest

from dbnd import new_dbnd_context
from dbnd._core.settings import DatabandSystemConfig, EnvConfig
from dbnd.tasks.basics import dbnd_sanity_check
from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig


logger = logging.getLogger(__name__)


@pytest.mark.gcp
@pytest.mark.gcp_k8s
@pytest.mark.skip(reason="Waiting for kubernetes integration")
class TestGcpK8sFlow(object):
    def test_simple_driver(self):
        with new_dbnd_context(
            conf={
                DatabandSystemConfig.env: "gcp_k8s",
                EnvConfig.driver_engine: "local_engine",
                EnvConfig.task_engine: "gcp_k8s_engine",
                KubernetesEngineConfig.debug: True,
            }
        ):
            t = dbnd_sanity_check.task(task_version="now")
            assert t.task_env.task_name == "gcp_k8s"
            t.dbnd_run()
