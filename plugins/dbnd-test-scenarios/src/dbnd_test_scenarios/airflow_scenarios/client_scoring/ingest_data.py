from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dbnd_test_scenarios.airflow_scenarios.airflow_scenarios_config import (
    dag_task_output,
)
from dbnd_test_scenarios.airflow_scenarios.utils import default_args_dbnd_scenarios_dag
from dbnd_test_scenarios.pipelines.client_scoring.ingest_data import (
    run_clean_piis,
    run_create_report,
    run_dedup_records,
    run_enrich_missing_fields,
    run_get_customer_data,
)


def publish_result(path):
    return path


def build_partner_ingest_dag(partner):
    """
    Run only from DAG context
    :param partner:
    :return:
    """

    def dag_task_output_partition_csv(name):
        return dag_task_output("%s.%s.{{ds}}.csv" % (partner, name))

    p_dto_csv = dag_task_output_partition_csv

    customer_data_op = PythonOperator(
        task_id="get_customer_data",
        python_callable=run_get_customer_data,
        op_kwargs={
            "partner_name": partner,
            "output_path": p_dto_csv("get_customer_data"),
            "target_date_str": "{{ds}}",
        },
    )

    clean_pii_op = PythonOperator(
        task_id="clean_pii",
        python_callable=run_clean_piis,
        op_kwargs={
            "input_path": p_dto_csv("get_customer_data"),
            "output_path": p_dto_csv("clean_pii"),
            "pii_columns": ["name", "address", "phone"],
            "target_date_str": "{{ds}}",
        },
    )

    enrich_missing_fields_op = PythonOperator(
        task_id="enrich_missing_fields",
        python_callable=run_enrich_missing_fields,
        op_kwargs={
            "input_path": p_dto_csv("clean_pii"),
            "output_path": p_dto_csv("enrich_missing_fields"),
        },
    )

    dedup_records_op = PythonOperator(
        task_id="dedup_records",
        python_callable=run_dedup_records,
        op_kwargs={
            "input_path": p_dto_csv("enrich_missing_fields"),
            "output_path": p_dto_csv("dedup_records"),
            "columns": ["id", "name", "address", "phone"],
        },
    )
    customer_data_op >> clean_pii_op >> enrich_missing_fields_op >> dedup_records_op

    report_op = PythonOperator(
        task_id="report",
        python_callable=run_create_report,
        op_kwargs={
            "input_path": p_dto_csv("dedup_records"),
            "output_path": p_dto_csv("report"),
        },
    )
    dedup_records_op >> report_op

    publish_result_op = PythonOperator(
        task_id="published",
        python_callable=publish_result,
        op_kwargs={"path": p_dto_csv("dedup_records")},
    )
    dedup_records_op >> publish_result_op

    return enrich_missing_fields_op


with DAG(
    dag_id="airflow_scenario_partner", default_args=default_args_dbnd_scenarios_dag
) as customer_ingest_dag:
    build_partner_ingest_dag("airflow_scenario_partner")

# track_dag(customer_ingest_dag)
