import json
import logging


logger = logging.getLogger(__name__)


class DatabandError(Exception):
    """
    Base exception.
    """

    _default_show_exc_info = True

    def __init__(self, *args, **kwargs):
        help_msg = kwargs.pop("help_msg", None)
        nested_exceptions = kwargs.pop("nested_exceptions", None)
        show_exc_info = kwargs.pop("show_exc_info", self._default_show_exc_info)

        super(DatabandError, self).__init__(*args, **kwargs)

        self.help_msg = help_msg
        self.show_exc_info = show_exc_info

        from dbnd._core.errors.errors_utils import UserCodeDetector

        self.user_frame_info = (
            UserCodeDetector.build_code_detector().find_user_side_frame(
                user_side_only=True
            )
        )

        if nested_exceptions:
            if not isinstance(nested_exceptions, list):
                nested_exceptions = [nested_exceptions]
        else:
            nested_exceptions = []

        try:
            self.nested_exceptions = nested_exceptions
            from dbnd._core.configuration.environ_config import is_unit_test_mode

            if is_unit_test_mode():
                from dbnd._core.errors.errors_utils import nested_exceptions_str

                if nested_exceptions:
                    logger.exception(
                        "PYTEST INFO: There are nested exceptions : %s",
                        nested_exceptions_str(self),
                    )
        except Exception:
            logger.exception("Failed to print nested exceptions for unit test mode")


class WrapperDatabandError(DatabandError):
    """
    Implement the boilerplate required to have a DatabandError which wrapping another DatabandError.
    Use this when you want to create a DatabandError that can chain inner error inside but keep the properties
     of the inner error.
    """

    def __init__(self, message, inner_error, **kwargs):
        # type: (str, Exception, dict) -> WrapperDatabandError
        extended_msg = (
            "{message}, caused by: \n\t {cause_name}: {cause_message} ".format(
                message=message,
                cause_name=inner_error.__class__.__name__,
                cause_message=str(inner_error),
            )
        )
        super(WrapperDatabandError, self).__init__(extended_msg, **kwargs)
        self.inner_error = inner_error

        # try passing the help message of the cause to this error if current error is None
        try:
            self.help_msg = self.help_msg or self.inner_error.help_msg
        except AttributeError:
            pass


class DatabandBuildError(DatabandError):
    """
    Error while building task
    """

    _default_show_exc_info = False


class DatabandRuntimeError(DatabandError):
    """
    Error in task run
    """

    _default_show_exc_info = False


class DatabandSigTermError(DatabandError):
    """
    SIGTERM received by the process
    """

    _default_show_exc_info = False


class DatabandSystemError(Exception):
    """
    Databand error that should not happen!
    """

    _default_show_exc_info = True


class DatabandConfigError(DatabandError):
    _default_show_exc_info = False


class DatabandRunError(DatabandError):
    """
    Databand executor(airflow) failed to run some tasks
    """

    _default_show_exc_info = False

    def __init__(self, *args, **kwargs):
        run = kwargs.pop("run", None)

        super(DatabandRunError, self).__init__(*args, **kwargs)
        if run is None:
            from dbnd import get_databand_run

            run = get_databand_run()

        self.run = run


class DatabandFailFastError(DatabandRunError):
    """
    Databand executor has terminated because a task failed
    """


class ConfigLookupError(DatabandConfigError):
    """
    Error signifying that the wanted configuration wasn't found
    """

    _default_show_exc_info = False


class ParameterError(DatabandConfigError):
    """
    Base exception.
    """

    _default_show_exc_info = False


class ParseValueError(ParameterError):
    """
    Error signifying that there was a error whily parsing Parameter.
    """


class ValueTypeError(DatabandConfigError):
    """
    Base exception.
    """

    _default_show_exc_info = False


class ParseParameterError(ParameterError):
    """
    Error signifying that there was a error whily parsing Parameter.
    """


class MissingParameterError(ParameterError):
    """
    Error signifying that there was a missing Parameter.
    """


class UnknownParameterError(ParameterError):
    """
    Error signifying that an unknown Parameter was supplied.
    """


class DbndCanceledRunError(DatabandError):
    """
    Error thrown when canceling a run.
    """


class TaskClassException(DatabandError):
    _default_show_exc_info = False


class TaskClassNotFoundException(TaskClassException):
    pass


class TaskClassAmbigiousException(TaskClassException):
    pass


class TaskValidationError(DatabandError):
    pass


class DatabandApiError(DatabandError):
    def __init__(self, method, endpoint, resp_code, response, **kwargs):
        super(DatabandApiError, self).__init__(
            method, endpoint, resp_code, response, **kwargs
        )

        self.method = method
        self.endpoint = endpoint
        self.resp_code = resp_code
        self.response = response

    def __str__(self):
        try:
            parsed_json = json.loads(self.response)
            error_message = json.dumps(parsed_json, indent=4)
        except Exception:
            error_message = self.response

        return "Call failed to endpoint %s %s\nResponse code: %s\nServer error: %s" % (
            self.method,
            self.endpoint,
            self.resp_code,
            error_message.replace("\n", "\n\t"),
        )


class DatabandUnauthorizedApiError(DatabandApiError):
    """Api error indicate unauthorized http request (status code: 403 or 401)"""


class DatabandBadRequest(DatabandError):
    pass


class DatabandWebserverNotReachableError(DatabandError):
    """
    Indicate that Databand webserver is not reachable
    """


class DatabandAuthenticationError(DatabandWebserverNotReachableError):
    """Api error indicate we failed to authenticate to the server"""


class DatabandConnectionException(DatabandWebserverNotReachableError):
    """
    Error thrown when connecting with the server is not available.
    """
