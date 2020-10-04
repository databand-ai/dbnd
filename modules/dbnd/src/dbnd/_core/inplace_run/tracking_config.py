from dbnd._core.configuration.dbnd_config import config


def set_tracking_config_overide(use_dbnd_log=None):
    # 1. create proper DatabandContext so we can create other objects
    track_with_cache = config.getboolean("run", "tracking_with_cache")
    config_for_airflow = {
        "run": {
            "skip_completed": track_with_cache,
            "skip_completed_on_run": track_with_cache,
            "validate_task_inputs": track_with_cache,
            "validate_task_outputs": track_with_cache,
        },  # we don't want to "check" as script is task_version="now"
        "task": {
            "task_in_memory_outputs": not track_with_cache
        },  # do not save any outputs
        "core": {"tracker_raise_on_error": False},  # do not fail on tracker errors
    }
    if use_dbnd_log is not None:
        config_for_airflow["log"] = {"disabled": not use_dbnd_log}
    config.set_values(
        config_values=config_for_airflow, override=True, source="dbnd_tracking_config"
    )

    return
