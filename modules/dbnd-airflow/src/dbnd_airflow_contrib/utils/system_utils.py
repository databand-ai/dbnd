import logging
import re
import subprocess


logger = logging.getLogger(__name__)


def print_stack_trace(stack_frame):
    import traceback

    traceback.print_stack(stack_frame)


def print_cpu_memory_usage():
    import psutil

    cpu_usage_percent = psutil.cpu_percent(interval=1)
    virtual_memory = psutil.virtual_memory()
    last_minute_load, last_5_minute_load, last_15_minute_load = [
        x / psutil.cpu_count() * 100 for x in psutil.getloadavg()
    ]

    logger.info(
        """
                Cpu usage %%: %s"
                "Virtual memory: %s"
                "Last minute cpu load %%: %s"
                "Last 5 minute cpu load %%: %s"
                "Last 15 minute cpu load %%: %s"
                """
        % (
            cpu_usage_percent,
            virtual_memory,
            last_minute_load,
            last_5_minute_load,
            last_15_minute_load,
        )
    )


def print_dmesg():
    try:
        human_dmesg()
    except Exception as e:
        logger.info("Could not get dmesg data! Exception: %s", e)


_datetime_format = "%Y-%m-%d %H:%M:%S"
_dmesg_line_regex = re.compile("^\[(?P<time>\d+\.\d+)\](?P<line>.*)$")


def human_dmesg():
    from datetime import datetime, timedelta

    now = datetime.now()
    uptime_diff = None
    try:
        with open("/proc/uptime") as f:
            uptime_diff = f.read().strip().split()[0]
    except IndexError:
        return
    else:
        try:
            uptime = now - timedelta(
                seconds=int(uptime_diff.split(".")[0]),
                microseconds=int(uptime_diff.split(".")[1]),
            )
        except IndexError:
            return

    dmesg_data = subprocess.check_output(["dmesg"]).decode()
    for line in dmesg_data.split("\n"):
        if not line:
            continue
        match = _dmesg_line_regex.match(line)
        if match:
            try:
                seconds = int(match.groupdict().get("time", "").split(".")[0])
                nanoseconds = int(match.groupdict().get("time", "").split(".")[1])
                microseconds = int(round(nanoseconds * 0.001))
                line = match.groupdict().get("line", "")
                t = uptime + timedelta(seconds=seconds, microseconds=microseconds)
            except IndexError:
                pass
            else:
                logger.info("[%s]%s" % (t.strftime(_datetime_format), line))
