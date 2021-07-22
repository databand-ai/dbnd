import mock
import pytest

from dbnd import Config, parameter
from targets.types import NullableStr


@pytest.mark.skip("need to be fixed")
class TestParameterWarnings(object):
    @mock.patch("parameters.warnings")
    def test_warn_on_default_none(self, warnings):
        class TestConfig(Config):
            param = parameter(default=None)[str]

        TestConfig()
        warnings.warn.assert_called_once_with(
            'Parameter "param" with value "None" is not of type string.'
        )

    @mock.patch("parameters.warnings")
    def test_no_warn_on_string(self, warnings):
        class TestConfig(Config):
            param = parameter(default=None)[str]

        TestConfig(param="str")
        warnings.warn.assert_not_called()

    @mock.patch("parameters.warnings")
    def test_no_warn_on_none_in_optional(self, warnings):
        class TestConfig(Config):
            param = parameter(default=None)[NullableStr]

        TestConfig()
        warnings.warn.assert_not_called()

    @mock.patch("parameters.warnings")
    def test_no_warn_on_string_in_optional(self, warnings):
        class TestConfig(Config):
            param = parameter(default=None)[NullableStr]

        TestConfig(param="value")
        warnings.warn.assert_not_called()

    @mock.patch("parameters.warnings")
    def test_warn_on_bad_type_in_optional(self, warnings):
        class TestConfig(Config):
            param = parameter[NullableStr]

        TestConfig(param=1)
        warnings.warn.assert_called_once_with(
            'OptionalParameter "param" with value "1" is not of type string or None.'
        )
