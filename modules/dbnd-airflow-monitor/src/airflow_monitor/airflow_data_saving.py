import json
import logging
import sys

from time import sleep
from timeit import default_timer

from airflow_monitor.common.metric_reporter import METRIC_REPORTER
from dbnd import get_databand_context
from dbnd._core.errors.base import DatabandConnectionException
from dbnd._core.utils.http.retry_policy import get_retry_policy
from dbnd.api.shared_schemas.airflow_monitor import airflow_server_info_schema


logger = logging.getLogger(__name__)


def _is_unique_constr_error(ex):
    ex_resp = type(ex)
    if hasattr(ex, "response"):
        # Handle HTTP exceptions
        ex_resp = ex.response

    return "IntegrityError" in str(ex_resp) and "unique" in str(ex).lower()


def save_airflow_server_info(airflow_server_info):
    with METRIC_REPORTER.dbnd_api_response_time.labels(
        airflow_server_info.base_url, "save_airflow_server_info"
    ).time():
        logging.info("Sending airflow server info to databand web server")
        marshalled = airflow_server_info_schema.dump(airflow_server_info.__dict__)
        api_client = get_databand_context().databand_api_client

        def failure_handler(exc, retry_policy, retry_number):
            logger.error(
                "Could not connect to databand api server on host: %s",
                api_client._api_base_url,
            )
            if retry_policy.should_retry(500, None, retry_number):
                logger.info(
                    "Trying again in %d seconds",
                    retry_policy.seconds_to_sleep(retry_number),
                )

        retry_policy = get_retry_policy("save_airflow_server_info", max_retries=-1)

        try:
            start = default_timer()
            api_client.api_request(
                "airflow_monitor/save_airflow_server_info",
                marshalled.data,
                retry_policy=retry_policy,
                failure_handler=failure_handler,
            )
            end = default_timer()
            logging.info(
                "Finished sending airflow server info to databand web server. Total time: %s",
                end - start,
            )
            return
        except Exception as e:
            logger.exception("Failed to update airflow server info.", exc_info=e)
            return


def _call_tracking_store(tracking_store_function, **kwargs):
    """ Returns whether succeeded or not """
    while True:
        try:
            tracking_store_function(**kwargs)
            return True
        except DatabandConnectionException as dce:
            logger.info("Exception happened: %s", dce)
            logger.info("Inner exception: %s", dce.nested_exceptions)
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


def save_airflow_monitor_data(airflow_data, base_url, last_sync_time):
    json_data = json.dumps(airflow_data)
    logger.info(
        "Sending Airflow data to databand web server. Total size: %s kB",
        sys.getsizeof(json_data) / 1000,
    )
    tracking_service = get_databand_context().tracking_store_allow_errors
    start = default_timer()

    with METRIC_REPORTER.dbnd_api_response_time.labels(
        base_url, "save_airflow_monitor_data"
    ).time():
        _call_tracking_store(
            tracking_store_function=tracking_service.save_airflow_monitor_data,
            airflow_monitor_data=json_data,
            airflow_base_url=base_url,
            last_sync_time=last_sync_time,
        )
    end = default_timer()
    logger.info(
        "Finished sending Airflow data to databand web server. Total time: %s",
        end - start,
    )
