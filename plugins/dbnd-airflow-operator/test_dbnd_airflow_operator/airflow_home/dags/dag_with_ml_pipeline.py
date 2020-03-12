#  UN-COMMENT TO ACTIVATE
#
#
# from datetime import timedelta
#
# from airflow import DAG
# from airflow.utils.dates import days_ago
# from pandas import DataFrame
#
# from dbnd import pipeline
# from dbnd._core.current import try_get_current_task
# from dbnd_examples.pipelines.wine_quality.wine_quality_decorators_py3 import (
#     predict_wine_quality,
# )
#
#
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": days_ago(2),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
#     # 'dbnd_config': {
#     #      "my_task.p_int": 4
#     # }
# }
#
#
# @pipeline
# def predict_wine_quality_with_airflow(
#     data: DataFrame = None,
#     alpha: float = 0.5,
#     l1_ratio: float = 0.5,
#     good_alpha: bool = False,
# ):
#     from airflow.operators.bash_operator import BashOperator
#
#     native_op = BashOperator(task_id="native_airflow_op", bash_command="echo hello")
#     try_get_current_task().set_upstream(native_op)
#     return predict_wine_quality(
#         data=data, alpha=alpha, l1_ratio=l1_ratio, good_alpha=good_alpha
#     )
#
#
# with DAG(dag_id="dbnd_predict", default_args=default_args) as dag_operators:
#     predict_wine_quality(
#         data="../../../../examples/data/wine_quality.csv"
#     )
