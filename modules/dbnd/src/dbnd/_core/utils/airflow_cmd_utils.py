def generate_airflow_cmd(dag_id, task_id, execution_date, is_root_task=False):
    return "airflow {sub_command} {dag_id}{task_id} {start_date} {end_date}".format(
        sub_command="backfill" if is_root_task else "test",
        dag_id=dag_id,
        task_id=" %s" % task_id if not is_root_task else "",
        start_date="-s %s" % execution_date,
        end_date="-e %s" % execution_date,
    )


def generate_airflow_func_call(
    dag_id,
    schedule_interval,
    execution_date,
    task_type=None,
    task_id=None,
    task_retries=None,
    task_command=None,
    is_root_task=False,
):
    if is_root_task:
        return (
            "DAG(dag_id='{dag_id}', default_args=args, schedule_interval='{schedule_interval}')"
            ".run(start_date={start_date}, end_date={end_date}, execution_date={execution_date})".format(
                dag_id=dag_id,
                schedule_interval=schedule_interval,
                start_date=execution_date,
                end_date=execution_date,
                execution_date=execution_date,
            )
        )
    elif task_id is not None:
        return "{operator}(task_id='{task_id}', retries='{retries}', {command}))".format(
            operator=task_type,
            task_id=task_id,
            retries=task_retries,
            command=task_command,
        )
