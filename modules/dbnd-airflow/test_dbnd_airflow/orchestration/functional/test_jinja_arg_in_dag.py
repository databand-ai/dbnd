# from airflow import DAG
#
# from test_dbnd_airflow.airflow_home.dags.dag_test_examples import default_args_test
# from test_dbnd_airflow.airflow_home.dags.dag_with_xcom_pipeline import (
#     bool_to_string,
#     my_second_pipeline,
#     my_xcom_pipeline,
# )
# from test_dbnd_airflow.functional.utils import read_xcom_result_value, run_and_get
#
#
# with DAG(dag_id="simple_jinja_dag", default_args=default_args_test) as simple_jinja_dag:
#     my_second_pipeline(p_date="{{ ts }}")
#
# with DAG(
#     dag_id="nested_pipeline_dag", default_args=default_args_test
# ) as nested_pipeline_dag:
#     my_xcom_pipeline(p_date="{{ ts }}")
#
# with DAG(
#     dag_id="jinja_arg_to_task", default_args=default_args_test
# ) as jinja_to_task_dag:
#     bool_to_string(p_bool="{{ test_mode }}")
#
#
# class TestFunctionalDagRun(object):
#     def test_simple_pipeline_jinja_arg(self):
#         results = run_and_get(simple_jinja_dag, "my_second_pipeline")
#         parsed_result = read_xcom_result_value(results)
#         assert "SEPARATOR" in parsed_result
#
#     def test_nested_pipeline_jinja_arg(self):
#         results = run_and_get(nested_pipeline_dag, "my_xcom_pipeline")
#         first_result = read_xcom_result_value(results, "result_1")
#         second_result = read_xcom_result_value(results, "result_2")
#         assert "SEPARATOR" in first_result
#         assert "SEPARATOR" not in second_result
#
#     def test_jinja_arg_to_task(self):
#         results = run_and_get(jinja_to_task_dag, "bool_to_string")
#         parsed_result = read_xcom_result_value(results)
#         assert parsed_result == "False"
