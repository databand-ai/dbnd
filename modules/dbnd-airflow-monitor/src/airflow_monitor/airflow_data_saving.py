import json
import logging

from time import sleep

import prometheus_client

from dbnd._core.errors.base import DatabandConnectionException
from dbnd.api.shared_schemas.airflow_monitor import airflow_server_info_schema


logger = logging.getLogger(__name__)
prometheus_dbnd_api = prometheus_client.Summary("af_monitor_dbnd_api_response_time", "")


def _is_unique_constr_error(ex):
    ex_resp = type(ex)
    if hasattr(ex, "response"):
        # Handle HTTP exceptions
        ex_resp = ex.response

    return "IntegrityError" in str(ex_resp) and "unique" in str(ex).lower()


@prometheus_dbnd_api.time()
def save_airflow_server_info(airflow_server_info, api_client):
    logging.info("Updating airflow server info.")
    marshalled = airflow_server_info_schema.dump(airflow_server_info.__dict__)

    while True:
        try:
            api_client.api_request(
                "airflow_monitor/save_airflow_server_info", marshalled.data
            )
            return
        except DatabandConnectionException as e:
            logger.error(
                "Could not connect to databand api server on host: %s",
                api_client._api_base_url,
            )
            logger.info("Trying again in %d seconds", 5)
            sleep(5)
        except Exception as e:
            logger.exception("Failed to update airflow server info.", exc_info=e)
            return


@prometheus_dbnd_api.time()
def _call_tracking_store(tracking_store_function, **kwargs):
    """ Returns whether succeeded or not """
    while True:
        try:
            tracking_store_function(**kwargs)
            return True
        except DatabandConnectionException:
            logger.info("Trying again in %d seconds", 5)
            sleep(5)
        except Exception as e:
            if _is_unique_constr_error(e):
                logger.debug(
                    "Failed %s for %s. (Data already exists)",
                    tracking_store_function.__name__,
                    kwargs,
                )
            else:
                logger.exception(
                    "Failed %s for %s",
                    tracking_store_function.__name__,
                    kwargs,
                    exc_info=e,
                )
            return False


def save_airflow_monitor_data(airflow_data, tracking_service, base_url, last_sync_time):
    logger.info("Saving data received from Airflow plugin")
    json_data = json.dumps(airflow_data)
    _call_tracking_store(
        tracking_store_function=tracking_service.save_airflow_monitor_data,
        airflow_monitor_data=json_data,
        airflow_base_url=base_url,
        last_sync_time=last_sync_time,
    )
    logger.debug("Finished saving Airflow data")
