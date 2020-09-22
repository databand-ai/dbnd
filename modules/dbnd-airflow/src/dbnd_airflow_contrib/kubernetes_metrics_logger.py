class KubernetesMetricsLogger(object):
    def __init__(self):
        pass

    def log_pod_information(self, task, pod_name, node_name):
        task.log_system_metric("pod_name", pod_name)
        task.log_system_metric("node_name", node_name)

    def log_pod_started(self, task):
        import datetime

        timestamp = datetime.datetime.utcnow().isoformat()
        task.log_system_metric("pod_started_execution", timestamp)

    def log_pod_deleted(self, task):
        import datetime

        timestamp = datetime.datetime.utcnow().isoformat()
        task.log_system_metric("pod_finished_execution", timestamp)
