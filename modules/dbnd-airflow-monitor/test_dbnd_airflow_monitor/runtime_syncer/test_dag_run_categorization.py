import random
import string

from airflow_monitor.common import AirflowDagRun
from airflow_monitor.syncer.runtime_syncer import categorize_dag_runs


def random_text(n=10):
    return "".join(random.choice(string.ascii_letters) for _ in range(n))


def gen_dag_run(id_, **kwargs):
    kwargs.setdefault("dag_id", random_text())
    kwargs.setdefault("execution_date", random_text())
    kwargs.setdefault("state", random.choice(["RUNNING", "SUCCESS", "FAILED"]))
    kwargs.setdefault("is_paused", random.choice([False, True]))

    kwargs.setdefault("has_updated_task_instances", random.choice([False, True]))
    if kwargs["has_updated_task_instances"]:
        kwargs.setdefault("max_log_id", random.randint(1, 10000))
        kwargs.setdefault("events", random.choice(["run"]))
    else:
        kwargs.setdefault("max_log_id", None)
        kwargs.setdefault("events", None)

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

    def test_020_view_only_events(self):
        # not RUNNING in dbnd nor in airflow, view only event => just update
        dag_runs = gen_dag_runs(0, 10, events="graph", state="SUCCESS") + gen_dag_runs(
            10, 20, events="graph", state="FAILED"
        )
        to_init, to_update = categorize_dag_runs(dag_runs, [100, 101],)
        assert not to_init
        assert dag_runs == to_update

    def test_021_view_only_events(self):
        # not RUNNING in dbnd but RUNNING in airflow, view only event => init
        dag_runs = gen_dag_runs(10, events="graph", state="RUNNING")
        to_init, to_update = categorize_dag_runs(dag_runs, [100, 101])
        assert dag_runs == to_init
        assert not to_update

    def test_022_view_only_events(self):
        # RUNNING in dbnd, view only event => update
        dag_runs = gen_dag_runs(
            0, 10, events="graph", state="RUNNING", has_updated_task_instances=True
        ) + gen_dag_runs(10, 20, events="graph", state="SUCCESS")
        to_init, to_update = categorize_dag_runs(dag_runs, list(range(20)))
        assert not to_init
        assert dag_runs == to_update

    def test_030_update_changed(self):
        dag_runs = gen_dag_runs(10, has_updated_task_instances=True)
        to_init, to_update = categorize_dag_runs(dag_runs, list(range(10)))
        assert not to_init
        assert dag_runs == to_update

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
