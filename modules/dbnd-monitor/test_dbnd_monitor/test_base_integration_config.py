# © Copyright Databand.ai, an IBM Company 2024


from test_dbnd_monitor.suites import (
    WhenComponentErrorDoesNotSupported,
    WhenComponentErrorSupportedByBothSides,
    WhenComponentErrorSupportedByMonitorOnly,
    WhenComponentErrorSupportedByWebServerOnly,
)


class TestSupportNotExpectedByDefaultImpl(WhenComponentErrorDoesNotSupported):
    def test_component_error_not_supported(self):
        assert self.generic_syncer.config.component_error_support is False


class TestSupportNotExpectedExplicitlyMonitor(WhenComponentErrorSupportedByMonitorOnly):
    def test_component_error_not_supported(self):
        assert self.generic_syncer.config.component_error_support is False


class TestSupportNotExpectedExplicitlyWeb(WhenComponentErrorSupportedByWebServerOnly):
    def test_component_error_not_supported(self):
        assert self.generic_syncer.config.component_error_support is False


class TestSupportExpected(WhenComponentErrorSupportedByBothSides):
    def test_env_component_error_support(self):
        assert self.generic_syncer.config.component_error_support is True
