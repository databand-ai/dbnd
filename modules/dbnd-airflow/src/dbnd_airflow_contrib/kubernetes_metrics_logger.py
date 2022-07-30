# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.current import is_verbose, try_get_databand_run
from dbnd_airflow_contrib.utils.system_utils import (
    print_cpu_memory_usage,
    print_dmesg,
    print_stack_trace,
)


logger = logging.getLogger(__name__)


class KubernetesMetricsLogger(object):
    def __init__(self):
        pass

    def log_pod_running(self, task, node_name):
        task.log_system_metric("node_name", node_name)

    def log_pod_submitted(self, task, pod_name):
        import datetime

        timestamp = datetime.datetime.utcnow().isoformat()
        task.log_system_metric("pod_submitted_at", timestamp)
        task.log_system_metric("pod_name", pod_name)

    def log_pod_finished(self, task):
        import datetime

        timestamp = datetime.datetime.utcnow().isoformat()
        task.log_system_metric("pod_finished_at", timestamp)


def log_pod_events_on_sigterm(stack_frame):
    print_stack_trace(stack_frame)
    print_driver_events()
    print_cpu_memory_usage()
    if is_verbose():
        print_dmesg()


def print_driver_events():
    try:
        dbnd_run = try_get_databand_run()
        engine_config = dbnd_run.run_executor.remote_engine
        kube_client = engine_config.get_kube_client()
        from socket import gethostname

        driver_pod_name = gethostname()
        logger.info("Driver pod name is %s" % (driver_pod_name,))
        field_selector = "involvedObject.name=%s" % driver_pod_name
        logger.info("Field selector is %s" % (field_selector,))
        driver_events = kube_client.list_namespaced_event(
            namespace=engine_config.namespace, field_selector=field_selector
        )
        logger.info("Found %s driver events" % (len(driver_events.items),))
        for event in driver_events.items:
            message = create_log_message_from_event(event)
            logger.info(message)
    except Exception as e:
        logger.info("Could not retrieve driver events! Exception: %s", e)


def create_log_message_from_event(event):
    return """
    Pod: %s
    Event count: %s
    Message: %s
    Type: %s
    Action: %s
    Reason: %s
    Source: %s
    Event time: %s
    First timestamp: %s
    Last timestamp: %s
    Reporting component: %s
    Reporting instance: %s
    Involved object: %s
    """ % (
        event.metadata.name,
        event.count,
        event.message,
        event.type,
        event.action,
        event.reason,
        event.source,
        event.event_time,
        event.first_timestamp,
        event.last_timestamp,
        event.reporting_component,
        event.reporting_instance,
        event.involved_object,
    )
