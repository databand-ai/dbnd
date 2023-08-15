# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.log.buffered_log_manager import BufferedLogManager


class BufferedMemoryHandler(logging.Handler):
    def __init__(self, max_tail_bytes, max_head_bytes):
        logging.Handler.__init__(self)
        self.buffer_manager = BufferedLogManager(
            max_tail_bytes=max_tail_bytes, max_head_bytes=max_head_bytes
        )
        self.set_name("dbnd")

    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        The record is then written to the stream with a trailing newline.  If
        exception information is present, it is formatted using
        traceback.print_exception and appended to the stream.  If the stream
        has an 'encoding' attribute, it is used to determine how to do the
        output to the stream.
        """
        try:
            msg = self.format(record)
            self.buffer_manager.add_log_msg(msg)
        except RecursionError:  # See issue 36272
            raise
        except Exception:
            self.handleError(record)

    def get_log_body(self):
        return self.buffer_manager.get_log_body()
