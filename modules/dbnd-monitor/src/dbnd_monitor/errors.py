# © Copyright Databand.ai, an IBM Company 2022
from dbnd._core.errors import DatabandError


class ClientConnectionError(DatabandError):
    """
    This type of error does not get sent to sentry!!!
    """
