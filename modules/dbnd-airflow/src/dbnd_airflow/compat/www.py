from airflow.www import app

from dbnd_airflow.contants import AIRFLOW_BELOW_2


if AIRFLOW_BELOW_2:
    from airflow import settings
    from airflow.www_rbac.api.experimental.endpoints import (
        api_experimental,
        requires_authentication,
    )

    if settings.RBAC:
        from airflow.www_rbac import views
    else:
        from airflow.www import views
else:
    from airflow.www import views
    from airflow.www.api.experimental.endpoints import (
        api_experimental,
        requires_authentication,
    )


def get_apps_to_patch():
    apps = [app]
    if AIRFLOW_BELOW_2:
        from airflow.www_rbac import app as rbac_app

        apps.append(rbac_app)
    return apps


def get_app_for_create_app():
    if AIRFLOW_BELOW_2:
        from airflow.www_rbac import rbac_app

        return rbac_app
    return app
