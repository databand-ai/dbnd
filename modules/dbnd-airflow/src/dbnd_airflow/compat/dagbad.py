from dbnd_airflow.contants import AIRFLOW_BELOW_2


if AIRFLOW_BELOW_2:
    from airflow.www_rbac.views import dagbag
else:
    from airflow.models import dagbag
