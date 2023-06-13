# © Copyright Databand.ai, an IBM Company 2022
import logging

from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd._core.current import get_databand_context
from dbnd.api.databand_client import DatabandClient
from dbnd.utils.api_client import ApiClient


def get_tracking_api_client() -> ApiClient:
    """
    There is a side affect,
    this code initialize DBND configs if required
    :return: initialized tracking API client
    """
    # let's make sure that configuraiton is loaded
    dbnd_bootstrap()
    context = get_databand_context()
    return context.databand_api_client


def get_databand_client() -> DatabandClient:
    """
    There is a side affect,
    this code initialize DBND configs if required
    :return: initialized Databadn Client
    """
    # let's make sure that configuraiton is loaded
    dbnd_bootstrap()
    logging.info("Current context for api client: %s", get_databand_context())
    return DatabandClient.build_databand_client()
