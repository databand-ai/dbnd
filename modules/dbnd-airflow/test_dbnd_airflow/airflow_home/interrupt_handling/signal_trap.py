import signal
import sys
import threading
import time

from threading import Thread


stop_requested = False

current_work = 0


def msg(s):
    sys.stderr.write("%s:  %s\n" % (threading.get_ident(), s))
    sys.stderr.flush()


def stop():
    global current_work
    msg("Doing something with current: %s" % (current_work))


def sig_handler(signum, frame):
    msg("handling signal: %s\n" % (signum))
    msg("handling frame: %s\n" % (frame))
    #
    # global stop_requested
    # stop_requested = True
    t = Thread(target=stop)
    t.start()
    t.join()
    raise Exception()


def run():
    global current_work
    msg("run started\n")
    try:
        while not stop_requested:
            current_work += 1
            msg("running %s \n" % current_work)
            time.sleep(2)
    except KeyboardInterrupt:
        msg("run KeyboardInterrupt\n")
    except Exception:
        msg("run Exception\n")

    msg("run exited\n")


signal.signal(signal.SIGTERM, sig_handler)
signal.signal(signal.SIGINT, sig_handler)

msg("starting\n")
main_thread = Thread(target=run)
main_thread.start()
main_thread.join()
msg("join completed\n")
