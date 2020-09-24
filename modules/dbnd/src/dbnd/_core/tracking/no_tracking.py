def dont_track(obj):
    """
    will not track obj. support airflow operators, DAGs, and functions.
    allows excluding from tracking when using dbnd-airflow-auto-tracking, track_modules, and track_dag.
    can be used as a function and decorator as shown in examples below.

    Usage examples:
        dag = DAG()
        with dag:
            operator = PythonOperator()

        dont_track(operator)
        dont_track(dag)

        @dont_track
        def f():
            pass

    """
    obj._dont_track = True
    return obj


def should_not_track(obj):
    if not hasattr(obj, "_dont_track"):
        return False
    return obj._dont_track
