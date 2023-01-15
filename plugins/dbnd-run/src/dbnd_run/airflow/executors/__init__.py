# © Copyright Databand.ai, an IBM Company 2022


class AirflowTaskExecutorType(object):
    airflow_inprocess = "airflow_inprocess"
    airflow_multiprocess_local = "airflow_multiprocess_local"
    airflow_kubernetes = "airflow_kubernetes"

    @staticmethod
    def all():
        return [k for k in dir(AirflowTaskExecutorType) if not k.startswith("_")]
