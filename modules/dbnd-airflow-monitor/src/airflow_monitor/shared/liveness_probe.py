import datetime
import os
import tempfile


LIVENESS_FILE_PATH_TEMPLATE = "/tmp/%s-monitor-alive"
MAX_TIME_DIFF_IN_SECONDS = 5 * 60


def create_liveness_file(monitor_type):
    latest_date = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    fd, path = tempfile.mkstemp()
    os.write(fd, latest_date.encode())
    os.close(fd)
    os.rename(path, LIVENESS_FILE_PATH_TEMPLATE.format(monitor_type))


def check_monitor_alive(monitor_type: str, max_time_diff=MAX_TIME_DIFF_IN_SECONDS):
    if not os.path.exists(LIVENESS_FILE_PATH_TEMPLATE.format(monitor_type)):
        raise Exception("Monitor is probably not alive!")

    with open(LIVENESS_FILE_PATH_TEMPLATE.format(monitor_type), "r") as f:
        timestamp_string = f.read()
        timestamp = datetime.datetime.strptime(timestamp_string, "%Y%m%d-%H%M%S")
        diff_in_seconds = (datetime.datetime.now() - timestamp).seconds

        if diff_in_seconds > max_time_diff:
            raise Exception("Monitor is probably not alive!")
