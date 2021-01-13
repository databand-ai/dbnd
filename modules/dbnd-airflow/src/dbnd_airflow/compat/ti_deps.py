from dbnd_airflow.contants import AIRFLOW_BELOW_2


if AIRFLOW_BELOW_2:
    from airflow.ti_deps.dep_context import RUNNABLE_STATES, RUNNING_DEPS
else:
    from airflow.ti_deps.dependencies_deps import RUNNING_DEPS
    from airflow.ti_deps.dependencies_states import RUNNABLE_STATES
