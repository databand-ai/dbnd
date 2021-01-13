from airflow.utils.state import State

from dbnd_airflow.contants import AIRFLOW_BELOW_2


def get_finished_states():
    return State.finished() if AIRFLOW_BELOW_2 else State.finished
