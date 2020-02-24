def dbnd_task_as_bash_operator(task_cls, name=None, cmd_line=None, **kwargs):
    from airflow.operators.bash_operator import BashOperator

    task_id = name or "run_%s" % task_cls.task_definition.task_family
    cmd_line = cmd_line or ""
    templated_command = "dbnd run {full_task_family} {cmd_line}".format(
        full_task_family=task_cls.task_definition.full_task_family, cmd_line=cmd_line
    )
    return BashOperator(task_id=task_id, bash_command=templated_command, **kwargs)


def dbnd_schedule_airflowDAG(
    task_cls,
    name=None,
    cmd_line="--task-target-date {{ ds }}",
    start_date=None,
    schedule_interval=None,
    **kwargs
):
    name = name or "scheduled_%s" % task_cls.task_definition.task_family
    from airflow import DAG

    dag = DAG(
        dag_id=name,
        start_date=start_date,
        schedule_interval=schedule_interval,
        **kwargs
    )

    dbnd_task_as_bash_operator(task_cls, name=name, cmd_line=cmd_line, dag=dag)
    return dag
