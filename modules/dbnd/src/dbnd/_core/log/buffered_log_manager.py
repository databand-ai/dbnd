# © Copyright Databand.ai, an IBM Company 2022

from dbnd._vendor.termcolor import colored


LINE_BRAKE = "\r\n"
MERGE_MSG = "\r\n...\r\n\r\nThe log body is truncated by databand, fetched {head_size} bytes for the `head` and {tail_size} bytes for the `tail` from the whole {total_log_size} bytes of the logs.\r\nControl the log preview length with LoggingConfig.preview_head_bytes and LoggingConfig.preview_tail_bytes \r\n\r\n...\r\n"
EMPTY_LOG_MSG = "Log is empty"
EMPTY_TRUNCATED_LOG_MSG = "Log truncated to empty"
MERGE_MSG_COLOR = "yellow"


class BufferedLogManager:
    """Buffered log body builder, for in-memory log buffering.

    This manager is responsible to handle how a final log body is supposed
    to look like, when there is a limitation to how much of the start and/or of the end
    of the log is needed to be shown.
    """

    terminator = "\n"

    def __init__(self, max_head_bytes=None, max_tail_bytes=None):
        from dbnd._core.settings import LoggingConfig

        self.initial_buffer = []
        self.initial_buffer_size = 0

        self.max_head_bytes = max_head_bytes
        self.max_tail_bytes = max_tail_bytes
        if self.max_head_bytes is None or self.max_tail_bytes is None:
            logger_config = LoggingConfig()
            self.max_head_bytes = max_head_bytes or logger_config.preview_head_bytes
            self.max_tail_bytes = max_tail_bytes or logger_config.preview_tail_bytes

        self.head_buffer = []
        self.current_head_bytes = 0
        self.trimmed_tail_buffer = []
        self.current_tail_bytes = 0

        self.tail_buffer = []

        self.total_log_size = 0

        self.tail_buffer_starts_with_partial_message = False

    @property
    def _is_initial_buffer_not_full(self) -> bool:
        return self.initial_buffer_size <= self.max_head_bytes + self.max_tail_bytes

    def _does_message_fit_in_initial_buffer(self, message_size) -> bool:
        return (
            self.initial_buffer_size + message_size
            <= self.max_head_bytes + self.max_tail_bytes
        )

    def add_log_msg(self, msg: str):
        """Add the message msg to the log body.

        Args:
            msg: current log message we are adding to the log body.
        """
        if self.max_head_bytes <= 0 and self.max_tail_bytes <= 0:
            return

        message_size = len(msg)
        self.total_log_size += message_size

        if self._is_initial_buffer_not_full:
            if self._does_message_fit_in_initial_buffer(message_size):
                self.initial_buffer.append(msg)
                self.initial_buffer_size += message_size
            else:
                self.initial_buffer_size = self.max_head_bytes + self.max_tail_bytes + 1
                for message in self.initial_buffer:
                    self._add_message_to_truncated_buffers(message)

                self._add_message_to_truncated_buffers(msg)
        else:
            self._add_message_to_truncated_buffers(msg)

    @property
    def minimum_acceptable_initial_partial_message_size(self) -> float:
        """The minimum acceptable size of an initial message, in the tail buffer.

        If the initial message's size in the tail buffer is smaller than this size, it gets deleted when requesting
        the log body.
        If it is larger than this size, than the partial message is kept, because of its substantial part out of the
        tail.
        """
        return self.max_tail_bytes * 0.5

    def _add_message_to_trimmed_tail(self, msg: str):
        message_size = len(msg)
        if self.current_tail_bytes + message_size <= self.max_tail_bytes:
            self.current_tail_bytes += message_size
            self.trimmed_tail_buffer.append(msg)
        elif self.max_tail_bytes > 0:
            # We have overflow in the tail
            bytes_to_rotate = message_size
            if self.current_tail_bytes < self.max_tail_bytes:
                bytes_to_rotate = (
                    self.current_tail_bytes + message_size - self.max_tail_bytes
                )
                self.current_tail_bytes = self.max_tail_bytes

            self.trimmed_tail_buffer.append(msg)
            # Rotation of bytes is needed to keep the newest max_tail_bytes bytes in the trimmed buffer
            while bytes_to_rotate > 0:
                if len(self.trimmed_tail_buffer[0]) <= bytes_to_rotate:
                    bytes_to_rotate -= len(self.trimmed_tail_buffer[0])
                    self.trimmed_tail_buffer = self.trimmed_tail_buffer[1:]
                    self.tail_buffer_starts_with_partial_message = False
                else:
                    self.trimmed_tail_buffer[0] = self.trimmed_tail_buffer[0][
                        bytes_to_rotate:
                    ]
                    self.tail_buffer_starts_with_partial_message = True
                    bytes_to_rotate = 0

    def get_log_body(self) -> str:
        """Builds the final log body.

        Returns:
            final log body, with a truncation message, if occurred.
        """
        if (
            self.current_head_bytes == 0
            and len(self.tail_buffer) == 0
            and self.initial_buffer_size > 0
        ):
            parts = [self.initial_buffer]
        else:
            if self.max_tail_bytes > 0:
                self._build_tail_for_preview()

            if (
                self.tail_buffer_starts_with_partial_message
                and len(self.trimmed_tail_buffer[0])
                < self.minimum_acceptable_initial_partial_message_size
            ):
                self.trimmed_tail_buffer.pop(0)

            parts = [self.head_buffer, self.trimmed_tail_buffer]
        # check if all the parts contains information
        if not any(parts):
            if self.max_head_bytes > 0 or self.max_tail_bytes > 0:
                return EMPTY_LOG_MSG
            # all the parts are truncated
            return EMPTY_TRUNCATED_LOG_MSG

        if not all(parts) and self.max_tail_bytes > 0 and self.max_head_bytes > 0:
            # tail buffer is an empty list
            parts = parts[0:1]

        # join each part with new lines
        joined_parts = [LINE_BRAKE.join(part) for part in parts]

        # building the merge message between head and tail
        merge_msg = colored(
            MERGE_MSG.format(
                head_size=self.max_head_bytes,
                tail_size=self.max_tail_bytes,
                total_log_size=self.total_log_size,
            ),
            MERGE_MSG_COLOR,
        )

        return merge_msg.join(joined_parts)

    def _build_tail_for_preview(self):
        self.current_tail_bytes = 0
        self.trimmed_tail_buffer = []
        for message in self.tail_buffer:
            self._add_message_to_trimmed_tail(message)

    def _add_message_to_truncated_buffers(self, msg: str):
        message_size = len(msg)
        if self.current_head_bytes < self.max_head_bytes:
            if self.current_head_bytes + message_size <= self.max_head_bytes:
                self.current_head_bytes += message_size
                self.head_buffer.append(msg)
            elif self.max_head_bytes > 0:
                # handle when there is some space in the head buffer, but we need to cut it off
                head_buffer_size_left = self.max_head_bytes - self.current_head_bytes
                self.head_buffer.append(msg[0:head_buffer_size_left])
                self.current_head_bytes = self.max_head_bytes
                if self.max_tail_bytes > 0:
                    self.tail_buffer_starts_with_partial_message = True
                    self._add_message_to_tail(msg[head_buffer_size_left:])

        elif self.max_tail_bytes > 0:
            self._add_message_to_tail(msg)

    def _add_message_to_tail(self, msg: str):
        self.tail_buffer.append(msg)
