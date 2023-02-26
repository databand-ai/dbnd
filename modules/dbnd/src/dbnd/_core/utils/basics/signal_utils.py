# © Copyright Databand.ai, an IBM Company 2022

import logging
import os
import signal

from dbnd._core.utils.platform import windows_compatible_mode
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)


def safe_signal(signalnum, handler):
    # wraps signal cmd, so it doesn't throws an exception
    # while running on windows/multithreaded environment
    try:
        return signal.signal(signalnum, handler)
    except Exception:
        logger.info("Failed to set an alert handler for '%s' signal", signalnum)
    return None


SIGQUIT_DUMP_DIR = "/tmp"


def sigquit_handler__dump_stack(sig, frame):
    """Helps debug deadlocks by printing stacktraces when this gets a SIGQUIT
    e.g. kill -s QUIT <PID> or CTRL+\
    """
    dump_trace(dump_file=True)
    return True


def dump_trace(dump_file=None):
    import os
    import sys
    import threading
    import traceback

    try:

        def _p(msg):
            sys.__stderr__.write("%s\n" % msg)

        _p("Dumping stack traces for all threads in PID {}".format(os.getpid()))
        id_to_name = {th.ident: th.name for th in threading.enumerate()}
        code = []
        for thread_id, stack in sys._current_frames().items():
            code.append(
                "\n# Thread: {}({})".format(id_to_name.get(thread_id, ""), thread_id)
            )
            for filename, line_number, name, line in traceback.extract_stack(stack):
                code.append(
                    'File: "{}", line {}, in {}'.format(filename, line_number, name)
                )
                if line:
                    code.append("  {}".format(line.strip()))
        traceback_data = "\n".join(code)
        _p("%s\n" % traceback_data)
        if dump_file is True:
            dump_file = os.path.join(
                SIGQUIT_DUMP_DIR,
                "dbnd.dump.%s.txt" % (utcnow().strftime("%Y%m%d-%H%M%S")),
            )
        if dump_file:
            with open(dump_file, "wt") as df_fp:
                df_fp.write(traceback_data)
            _p("Stack has been dumped into {}".format(dump_file))
        return traceback_data
    except Exception as e:
        print(
            "Couldn't report stack traces after reciving SIGQUIT! Exception: %s", str(e)
        )


def register_sigquit_stack_dump_handler():
    logger.warning(
        "SIGQUIT signal registered! Use `kill -%s %s` or CTRL+\\ for stacktrace dump.",
        signal.SIGQUIT,
        os.getpid(),
    )
    signal.signal(signal.SIGQUIT, sigquit_handler__dump_stack)


def register_graceful_sigterm():
    if windows_compatible_mode:
        return

    # Enables graceful shutdown when running inside docker/kubernetes and subprocess shutdown
    # By default the kill signal is SIGTERM while our code mostly expects SIGINT (KeyboardInterrupt)
    def sigterm_handler(sig, frame):
        pid = os.getpid()
        logger.info("Received signal in default signal handler. PID: %s", pid)
        os.kill(pid, signal.SIGINT)

    safe_signal(signal.SIGTERM, sigterm_handler)
