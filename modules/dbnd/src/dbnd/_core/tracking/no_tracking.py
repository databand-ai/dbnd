from dbnd._core.constants import AD_HOC_DAG_PREFIX


def dont_track(obj):
    """
    Will not track obj.

    support airflow operators, DAGs, and functions.
    allows excluding from tracking when using dbnd-airflow-auto-tracking, track_modules, and track_dag.
    can be used as a function and decorator as shown in examples below.
    As a function::

        dag = DAG()
        with dag:
            operator = PythonOperator()

        dont_track(operator)
        dont_track(dag)

    As a decorator::

        @dont_track
        def f():
            pass
    """
    obj._dont_track = True
    return obj


def should_not_track(obj):
    if hasattr(obj, "dag"):
        if getattr(obj.dag, "_dont_track", False):
            return True
        dag_id = getattr(obj.dag, "dag_id", None)
        if dag_id and dag_id.startswith(AD_HOC_DAG_PREFIX):
            return True
    return getattr(obj, "_dont_track", False)
