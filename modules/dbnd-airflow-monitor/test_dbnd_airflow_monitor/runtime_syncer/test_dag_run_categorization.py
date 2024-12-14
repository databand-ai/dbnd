# © Copyright Databand.ai, an IBM Company 2022

import random

from airflow_monitor.common.airflow_data import AirflowDagRun
from airflow_monitor.syncer.runtime_syncer import categorize_dag_runs

from . import random_text


def gen_dag_run(id_, **kwargs):
    kwargs.setdefault("dag_id", random_text())
    kwargs.setdefault("execution_date", random_text())
    kwargs.setdefault("state", random.choice(["running", "SUCCESS", "FAILED"]))
    kwargs.setdefault("is_paused", random.choice([False, True]))
    return AirflowDagRun(id_, **kwargs)


def gen_dag_runs(a, b=None, **kwargs):
    if b is None:
        a, b = 0, a
    return [gen_dag_run(i, **kwargs) for i in range(a, b)]


class TestDagRunCategorization:
    def test_01_simple_init(self):
        dag_runs = gen_dag_runs(10)
        to_init, to_update = categorize_dag_runs(dag_runs, [])
        assert dag_runs == to_init
        assert not to_update

    def test_020_finished_and_not_discovered_runs(self):
        # not RUNNING in dbnd nor in airflow, view only event => just update
        dag_runs = gen_dag_runs(0, 10, state="SUCCESS") + gen_dag_runs(
            10, 20, state="FAILED"
        )
        to_init, to_update = categorize_dag_runs(dag_runs, [100, 101])
        assert dag_runs == to_init
        assert not to_update

    def test_021_running_and_not_discovered_runs(self):
        # not RUNNING in dbnd but RUNNING in airflow, view only event => init
        dag_runs = gen_dag_runs(10, state="running")
        to_init, to_update = categorize_dag_runs(dag_runs, [100, 101])
        assert dag_runs == to_init
        assert not to_update

    def test_031_update_paused(self):
        dag_runs = gen_dag_runs(10, is_paused=True)
        to_init, to_update = categorize_dag_runs(dag_runs, list(range(10)))
        assert not to_init
        assert dag_runs == to_update

    def test_032_update_finished(self):
        dag_runs = gen_dag_runs(10, state="FAILED") + gen_dag_runs(10, state="SUCCESS")
        to_init, to_update = categorize_dag_runs(dag_runs, list(range(20)))
        assert not to_init
        assert dag_runs == to_update
