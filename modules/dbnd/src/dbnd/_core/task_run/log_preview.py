import io
import os

import six

from dbnd._core.settings import LoggingConfig
from dbnd._core.utils.string_utils import merge_dbnd_and_spark_logs
from dbnd._vendor.termcolor import colored


MERGE_MSG_COLOR = "yellow"

if six.PY2:
    from itertools import izip_longest as zip_longest
else:
    from itertools import zip_longest


LINE_BRAKE = "\r\n"
MERGE_MSG = "\r\n...\r\n\r\nThe log body is truncated by databand, fetched {head_size} bytes for the `head` and {tail_size} bytes for the `tail` from the whole {file_size} bytes of the file.\r\nFor full log access the log path {log_path} .\r\nControl the log preview length with LoggingConfig.preview_head_bytes and LoggingConfig.preview_tail_bytes \r\n\r\n...\r\n"
EMPTY_LOG_MSG = "Log truncated to empty"


def read_dbnd_log_preview(log_path, spark_log_path=None):
    """
    Reads the log_path using the current logging config
    """
    logger_config = LoggingConfig.current()
    max_head_size = logger_config.preview_head_bytes
    max_tail_size = logger_config.preview_tail_bytes

    # there is not need to run any of the logic if we don't need to read anything
    if max_head_size == 0 and max_tail_size == 0:
        return EMPTY_LOG_MSG

    if spark_log_path:
        # we need to read and than merge the content of dbnd log and spark long
        # using half of the max_bytes for each of the logs to make sure we are not exceeding the max after merging
        parts = merge_read_log_files(
            log_path, spark_log_path, max_head_size // 2, max_tail_size // 2,
        )

    else:
        # safely reading a single log file
        parts = read_head_and_tail(log_path, max_head_size, max_tail_size)

    # check if all the parts contains information
    if not any(parts):
        # all the parts are truncated
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

            if f.seekable():
                f.seek(-tail_size, io.SEEK_END)
            # removing the first line cause it may not be a complete line
            tail_content = f.readlines(tail_size)[1:]

            return _decode_lines(head_content), _decode_lines(tail_content)

        else:
            return (_decode_lines(f.readlines()),)


def merge_read_log_files(dbnd_log_path, spark_log_path, head_size, tail_size):
    """
    Handle the safe reading and merging of two log files.
    """
    # we create tuples of parts (heads together and tails together).
    # if there is no tail for only one of them will have : [(dbnd_head, spark_head), (dbnd_head, [])]
    parts = zip_longest(
        read_head_and_tail(dbnd_log_path, head_size, tail_size),
        read_head_and_tail(spark_log_path, head_size, tail_size),
        fillvalue=[],
    )

    # merging each part (heads, tails) separately
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
