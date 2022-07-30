# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.log.buffered_log_manager import (
    EMPTY_LOG_MSG,
    EMPTY_TRUNCATED_LOG_MSG,
    MERGE_MSG,
    MERGE_MSG_COLOR,
    BufferedLogManager,
)
from dbnd._vendor.termcolor import colored


class TestBufferedLogManager(object):
    @staticmethod
    def build_truncated_log_message(head, tail, head_bytes, tail_bytes, total_log_size):
        merge_msg = colored(
            MERGE_MSG.format(
                head_size=head_bytes,
                tail_size=tail_bytes,
                total_log_size=total_log_size,
            ),
            MERGE_MSG_COLOR,
        )

        return merge_msg.join([head, tail])

    def test_truncated_to_empty_with_empty_log(self):
        buffered_log_manager = BufferedLogManager(max_head_bytes=0, max_tail_bytes=0)
        log_preview = buffered_log_manager.get_log_body()
        assert EMPTY_TRUNCATED_LOG_MSG == log_preview

    def test_truncated_to_empty_with_log_messages(self):
        buffered_log_manager = BufferedLogManager(max_head_bytes=0, max_tail_bytes=0)
        buffered_log_manager.add_log_msg("test")

        log_preview = buffered_log_manager.get_log_body()
        assert EMPTY_TRUNCATED_LOG_MSG == log_preview

    def test_empty_log_message(self):
        buffered_log_manager = BufferedLogManager(
            max_head_bytes=1024, max_tail_bytes=1024
        )
        log_preview = buffered_log_manager.get_log_body()
        assert EMPTY_LOG_MSG == log_preview

    def test_only_head_no_truncation_preview(self):
        buffered_log_manager = BufferedLogManager(max_head_bytes=8, max_tail_bytes=0)
        buffered_log_manager.add_log_msg("test")

        log_preview = buffered_log_manager.get_log_body()
        assert "test" == log_preview

    def test_only_tail_no_truncation_preview(self):
        buffered_log_manager = BufferedLogManager(max_head_bytes=0, max_tail_bytes=8)
        buffered_log_manager.add_log_msg("test")

        log_preview = buffered_log_manager.get_log_body()
        assert "test" == log_preview

    def test_head_and_tail_no_truncation_preview(self):
        buffered_log_manager = BufferedLogManager(max_head_bytes=2, max_tail_bytes=2)
        buffered_log_manager.add_log_msg("test")

        log_preview = buffered_log_manager.get_log_body()
        assert "test" == log_preview

    def test_only_head_with_truncation_preview(self):
        buffered_log_manager = BufferedLogManager(max_head_bytes=10, max_tail_bytes=0)
        buffered_log_manager.add_log_msg("test1")
        buffered_log_manager.add_log_msg("test2")
        buffered_log_manager.add_log_msg("test3")

        actual_log_preview = buffered_log_manager.get_log_body()
        expected_log_preview = self.build_truncated_log_message(
            head="test1\r\ntest2",
            tail="",
            head_bytes=10,
            tail_bytes=0,
            total_log_size=15,
        )
        assert expected_log_preview == actual_log_preview

    def test_only_tail_with_truncation_preview(self):
        buffered_log_manager = BufferedLogManager(max_head_bytes=0, max_tail_bytes=10)
        buffered_log_manager.add_log_msg("test1")
        buffered_log_manager.add_log_msg("test2")
        buffered_log_manager.add_log_msg("test3")

        actual_log_preview = buffered_log_manager.get_log_body()
        expected_log_preview = self.build_truncated_log_message(
            head="",
            tail="test2\r\ntest3",
            head_bytes=0,
            tail_bytes=10,
            total_log_size=15,
        )
        assert expected_log_preview == actual_log_preview

    def test_head_and_tail_with_truncation_preview(self):
        buffered_log_manager = BufferedLogManager(max_head_bytes=5, max_tail_bytes=5)
        buffered_log_manager.add_log_msg("test1")
        buffered_log_manager.add_log_msg("test2")
        buffered_log_manager.add_log_msg("test3")

        actual_log_preview = buffered_log_manager.get_log_body()
        expected_log_preview = self.build_truncated_log_message(
            head="test1", tail="test3", head_bytes=5, tail_bytes=5, total_log_size=15
        )
        assert expected_log_preview == actual_log_preview

    def test_head_and_tail_with_truncation_preview_when_tail_starts_with_partial_message_removed(
        self,
    ):
        buffered_log_manager = BufferedLogManager(max_head_bytes=5, max_tail_bytes=5)
        buffered_log_manager.add_log_msg("test1")
        buffered_log_manager.add_log_msg("test2")
        buffered_log_manager.add_log_msg("1")
        buffered_log_manager.add_log_msg("2")
        buffered_log_manager.add_log_msg("3")
        buffered_log_manager.add_log_msg("4")

        actual_log_preview = buffered_log_manager.get_log_body()
        expected_log_preview = self.build_truncated_log_message(
            head="test1",
            tail="1\r\n2\r\n3\r\n4",
            head_bytes=5,
            tail_bytes=5,
            total_log_size=14,
        )
        assert expected_log_preview == actual_log_preview

    def test_head_and_tail_with_truncation_preview_when_tail_starts_with_partial_message_not_removed(
        self,
    ):
        buffered_log_manager = BufferedLogManager(max_head_bytes=5, max_tail_bytes=5)
        buffered_log_manager.add_log_msg("test1")
        buffered_log_manager.add_log_msg("test2")
        buffered_log_manager.add_log_msg("12")

        actual_log_preview = buffered_log_manager.get_log_body()
        expected_log_preview = self.build_truncated_log_message(
            head="test1",
            tail="st2\r\n12",
            head_bytes=5,
            tail_bytes=5,
            total_log_size=12,
        )
        assert expected_log_preview == actual_log_preview
