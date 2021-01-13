from dbnd_airflow.contants import AIRFLOW_BELOW_2


def get_kwargs_for_dag_run(
    run_id, execution_date, dag, state, external_trigger, conf,
):
    kwargs = {
        "run_id": run_id,
        "execution_date": execution_date,
        "start_date": dag.start_date,
        "external_trigger": external_trigger,
        "dag_id": dag.dag_id,
        "conf": conf,
    }

    kwargs.update(
        {"_state": state}
        if AIRFLOW_BELOW_2
        else {"state": state, "run_type": "dbnd_run"}
    )
    return kwargs
