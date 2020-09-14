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

        self.user_frame_info = UserCodeDetector.build_code_detector().find_user_side_frame(
            user_side_only=True
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


class ParameterError(DatabandConfigError):
    """
    Base exception.
    """

    _default_show_exc_info = False


class ParseValueError(ParameterError):
    """
    Error signifying that there was a error whily parsing Parameter.
    """

    pass


class ValueTypeError(DatabandConfigError):
    """
    Base exception.
    """

    _default_show_exc_info = False


class ParseParameterError(ParameterError):
    """
    Error signifying that there was a error whily parsing Parameter.
    """

    pass


class MissingParameterError(ParameterError):
    """
    Error signifying that there was a missing Parameter.
    """

    pass


class UnknownParameterError(ParameterError):
    """
    Error signifying that an unknown Parameter was supplied.
    """

    pass


class DatabandConnectionException(DatabandError):
    """
    Error thrown when connecting with the server is not available
    """

    pass


class TaskClassException(DatabandError):
    _default_show_exc_info = False


class TaskClassNotFoundException(TaskClassException):
    pass


class TaskClassAmbigiousException(TaskClassException):
    pass


class TaskValidationError(DatabandError):
    pass


class DatabandApiError(DatabandError):
    def __init__(self, method, endpoint, resp_code, response):
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


class DatabandBadRequest(DatabandError):
    pass
