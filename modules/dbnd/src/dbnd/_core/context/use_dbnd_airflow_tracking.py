# © Copyright Databand.ai, an IBM Company 2022


def should_use_airflow_monitor():
    try:
        import dbnd_airflow  # noqa: F401
    except ImportError:
        return False

    from dbnd_airflow.tracking.config import AirflowTrackingConfig

    tracking_config = AirflowTrackingConfig.from_databand_context()
    return tracking_config.af_with_monitor
