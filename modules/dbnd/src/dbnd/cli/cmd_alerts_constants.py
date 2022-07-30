# © Copyright Databand.ai, an IBM Company 2022

CREATE_HELP_MSG = """
Create alert definition for given pipeline\n
\n
EXAMPLES\n
        dbnd alerts create --pipeline my_pipeline --severity MEDIUM run-duration --op '==' -v 100\n
        dbnd alerts create --pipeline my_pipeline --severity HIGH run-duration --op ML --look-back 10 --sensitivity 4\n
        dbnd alerts create --pipeline my_pipeline --severity HIGH run-duration --op range --baseline 10 --range 4\n
        dbnd alerts create --pipeline my_pipeline --severity MEDIUM run-state -v failed\n
        dbnd alerts create --pipeline my_pipeline --severity CRITICAL ran-last-x-seconds -v 120\n
        dbnd alerts create --pipeline my_pipeline --severity MEDIUM --task my_pipeline_task task-state -v cancelled\n
        dbnd alerts create --pipeline my_pipeline --severity CRITICAL --task my_pipeline_task custom-metric --metric-name my_metric --op '<=' -v 100 --str-value false\n
"""

LIST_HELP_MSG = """
List alert definitions for a given pipeline\n
\n
EXAMPLE\n
        dbnd alerts list --pipeline my_pipeline
"""

DELETE_HELP_MSG = """
Delete alert definitions using one of [uid / pipeline / wipe (all)]\n
\n
EXAMPLES\n
        dbnd alerts delete --uid my_alert_uid\n
        dbnd alerts delete --pipeline my_pipeline\n
        dbnd alerts delete --wipe\n
"""

RANGE_ALERT_BASE_LINE_HELP = "Base number from which range alert will be measured"
RANGE_ALERT_RANGE_HELP = "Percent of baseline to include in range alerts."
ANOMALY_ALERT_LOOK_BACK_HELP = "Look Back number of runs used to train anomaly alerts."
ANOMALY_ALERT_SENSITIVITY_HELP = "Sensitivity of anomaly detection alerts."
METRIC_HELP = "Custom metric name to alert"
IS_STR_VALUE_HELP = (
    "Custom metric alert value type (true for string value, false for float)"
)
