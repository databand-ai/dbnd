import logging
import re
import subprocess


logger = logging.getLogger(__name__)


def print_stack_trace(stack_frame):
    try:
        import traceback

        traceback.print_stack(stack_frame)
    except Exception as e:
        logger.info("Could not print stack trace! Exception: %s", e)


def print_cpu_memory_usage():
    try:
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
    except Exception as e:
        logger.info("Could not read cpu and memory usage! Exception: %s", e)


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
    with open("/proc/uptime") as f:
        uptime_diff = f.read().strip().split()[0]
    uptime = now - timedelta(
        seconds=int(uptime_diff.split(".")[0]),
        microseconds=int(uptime_diff.split(".")[1]),
    )

    dmesg_data = subprocess.check_output(["dmesg"]).decode()
    for line in dmesg_data.split("\n"):
        if not line:
            continue
        match = _dmesg_line_regex.match(line)
        if match:
            seconds = int(match.groupdict().get("time", "").split(".")[0])
            nanoseconds = int(match.groupdict().get("time", "").split(".")[1])
            microseconds = int(round(nanoseconds * 0.001))
            line = match.groupdict().get("line", "")
            t = uptime + timedelta(seconds=seconds, microseconds=microseconds)
            logger.info("[%s]%s" % (t.strftime(_datetime_format), line))
