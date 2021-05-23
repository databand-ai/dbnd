import logging


logger = logging.getLogger(__name__)


def start_pycharm_debugger(host, port):
    """
    This is starting a debug connection to the pycharm at the address '{host}:{port}'
    """
    logger.info(
        "Starting debugging session with `pydevd_pycharm` on '{host}:{port}'".format(
            host=host, port=port
        )
    )

    try:
        import pydevd_pycharm
    except ImportError:
        logger.warning(
            "\n\tCouldn't import `pydevd_pycharm`.\n"
            "\tHelp:\tMake sure to install the relevant `pydevd_pycharm` version matching to your pycharm version"
        )
        return

    try:
        pydevd_pycharm.settrace(
            host, port=port, stdoutToServer=True, stderrToServer=True
        )
        return
    except ConnectionError:
        logger.exception(
            "\n\tCouldn't connect to pycharm host at '{host}:{port}'.\n"
            "\tHelp:\tPlease assert that there is a Python Debug Server listening on that address.\n"
            "\tFor more info:\thttps://www.jetbrains.com/help/pycharm/remote-debugging-with-product.html#remote-debug-config".format(
                host=host, port=port
            )
        )
        return
