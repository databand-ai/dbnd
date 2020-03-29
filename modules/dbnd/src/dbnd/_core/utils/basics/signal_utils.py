import logging
import signal


logger = logging.getLogger(__name__)


def safe_signal(signalnum, handler):
    # wraps signal cmd, so it doesn't throws an exception
    # while running on windows/multithreaded environment
    try:
        return signal.signal(signalnum, handler)
    except Exception:
        logger.info("Failed to set an alert handler for '%s' signal", signalnum)
    return None


def sigquit_handler(sig, frame):
    """Helps debug deadlocks by printing stacktraces when this gets a SIGQUIT
    e.g. kill -s QUIT <PID> or CTRL+\
    """
    import threading, traceback, sys, os, time

    try:
        print("Dumping stack traces for all threads in PID {}".format(os.getpid()))
        id_to_name = dict([(th.ident, th.name) for th in threading.enumerate()])
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
        print(traceback_data)
        temp_filename = f"/tmp/dbnd.dump.{time.strftime('%Y%m%d-%H%M%S')}.txt"
        with open(temp_filename, "wt") as dump_file:
            dump_file.write(traceback_data)
    except Exception as e:
        print(
            "Couldn't report stack traces after reciving SIGQUIT! Exception: %s", str(e)
        )
