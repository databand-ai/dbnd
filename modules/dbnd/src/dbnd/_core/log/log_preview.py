# © Copyright Databand.ai, an IBM Company 2022

import io
import logging
import os

import six

from dbnd._core.log import dbnd_log_debug
from dbnd._core.settings.tracking_log_config import TrackingLoggingConfig
from dbnd._core.utils.string_utils import merge_dbnd_and_spark_logs
from dbnd._vendor.termcolor import colored


logger = logging.getLogger(__name__)

MERGE_MSG_COLOR = "yellow"

if six.PY2:
    from itertools import izip_longest as zip_longest
else:
    from itertools import zip_longest


LINE_BRAKE = "\r\n"
MERGE_MSG = "\r\n...\r\n\r\nThe log body is truncated by databand, fetched {head_size} bytes for the `head` and {tail_size} bytes for the `tail` from the whole {file_size} bytes of the file.\r\nFor full log access the log path {log_path} .\r\nControl the log preview length with TrackingLoggingConfig.preview_head_bytes and LoggingConfig.preview_tail_bytes \r\n\r\n...\r\n"

# Frontend depends on this exact strings to show localized messages: don't change it!
EMPTY_LOG_MSG = "Log truncated to empty"


def read_dbnd_log_preview(log_path, spark_log_path=None):
    """
    Reads the log_path using the current logging config
    """
    logger_config = TrackingLoggingConfig()
    max_head_size = logger_config.preview_head_bytes
    max_tail_size = logger_config.preview_tail_bytes

    # there is not need to run any of the logic if we don't need to read anything
    if max_head_size == 0 and max_tail_size == 0:
        dbnd_log_debug("read_dbnd_log_preview: log head/tail=0, no log is sent to DBND")
        return EMPTY_LOG_MSG

    if spark_log_path:
        # we need to read and than merge the content of dbnd log and spark log
        # using half of the max_bytes for each of the logs to make sure we are not exceeding the max after merging
        parts = merge_read_log_files(
            log_path, spark_log_path, max_head_size // 2, max_tail_size // 2
        )

    else:
        # safely reading a single log file
        parts = read_head_and_tail(log_path, max_head_size, max_tail_size)

    # check if all the parts contains information
    if not any(parts):
        # all the parts are truncated
        dbnd_log_debug("read_dbnd_log_preview: no log found to send to DBND")
        return EMPTY_LOG_MSG

    # join each part with new lines
    joined_parts = [LINE_BRAKE.join(part) for part in parts]

    # building the merge message between head and tail
    merge_msg = colored(
        MERGE_MSG.format(
            head_size=max_head_size,
            tail_size=max_tail_size,
            file_size=os.path.getsize(log_path),
            log_path=log_path,
        ),
        MERGE_MSG_COLOR,
    )

    return merge_msg.join(joined_parts)


def adjust_log_lines_to_size(log_lines, size, reverse=False):
    # readlines() is not accurate, so need to perform an extra check on size
    # https://stackoverflow.com/a/14541029/6517749
    result = []
    current_size = 0
    for line in log_lines if not reverse else reversed(log_lines):
        current_size += len(line)
        if current_size <= size:
            result.append(line)
        else:
            break
    return result if not reverse else reversed(result)


def read_head_and_tail(path, head_size, tail_size):
    """
    Safe reading of the file:
    * Reads the whole file if its size is less than head_size + tail_size
    * Otherwise: Reads the head and the tail of the file separately.

    Parameters:
        path (str): the location of the file to read.
        head_size (int): max length of the the head part in bytes.
        tail_size (int): max length of the the tail part in bytes.

    Returns:
        A tuple with one (whole file) or two (head and tail) list of lines.
    """
    if not (_is_non_negative_int(head_size) and _is_non_negative_int(tail_size)):
        raise ValueError("tail and head max size has to be non negative integers")

    file_size = os.path.getsize(path)
    with io.open(path, "rb") as f:
        if head_size + tail_size < file_size:
            # readlines(0) returns all the lines but in our case we need no lines
            head_content = f.readlines(head_size) if head_size else []
            head_content = adjust_log_lines_to_size(head_content, head_size)

            if f.seekable():
                f.seek(-tail_size, io.SEEK_END)
            # removing the first line cause it may not be a complete line
            tail_content = f.readlines(tail_size)[1:]
            # reverse tail_content to remove an element from a beginning of log + reverse back
            tail_content = adjust_log_lines_to_size(
                tail_content, tail_size, reverse=True
            )

            return _decode_lines(head_content), _decode_lines(tail_content)

        else:
            return (_decode_lines(f.readlines()),)


def merge_read_log_files(dbnd_log_path, spark_log_path, head_size, tail_size):
    """
    Handle the safe reading and merging of two log files.
    """

    # if dbnd log read fails we want to move it up
    dbnd_log = read_head_and_tail(dbnd_log_path, head_size, tail_size)

    try:
        spark_log = read_head_and_tail(spark_log_path, head_size, tail_size)
    except Exception:
        logger.warning(
            "Failed to read dbnd spark log file {}".format(dbnd_log_path), exc_info=True
        )
        # if spark log read fails we want to return dbnd_log at least
        return dbnd_log

    # if both ok we merge them

    # 1) we create tuples of parts (heads together and tails together).
    #    if there is no tail for only one of them will have :
    #               [(dbnd_head, spark_head), (dbnd_tail, [])]
    parts = zip_longest(dbnd_log, spark_log, fillvalue=[])

    # 2) merging each part (heads, tails) separately
    merged_parts = []
    for part in parts:
        merged = merge_dbnd_and_spark_logs(*part)
        merged_parts.append(merged)

    return merged_parts


# some helper function to make the code cleaner
def _decode(s, encoding="utf-8", errors="strict"):
    """
    helper to transfer decode method to a function
    """
    return s.decode(encoding, errors)


def _decode_lines(lines):
    """
    helper to decodes multiple lines of strings
    """
    return [line.rstrip(LINE_BRAKE) for line in map(_decode, lines)]


def _is_non_negative_int(value):
    return isinstance(value, int) and value >= 0
