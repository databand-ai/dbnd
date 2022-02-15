import datetime

from dbnd._core.utils.timezone import convert_to_utc


class TestTimezone(object):
    def test_convert_to_utc_with_00_00(self):
        target = datetime.datetime.now()
        converted_value = convert_to_utc(target)
        converted_again = convert_to_utc(converted_value)
        converted_again = convert_to_utc(converted_again)
        assert converted_again
