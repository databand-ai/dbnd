from dbnd import parameter
from dbnd._core.settings.tracking_config import ValueTrackingLevel


class TestDocPerformantLogging:
    def test_doc(self):
        #### DOC START
        value_reporting_strategy = parameter(
            default=ValueTrackingLevel.SMART,
            description="Multiple strategies with different limitations on potentially expensive calculation for value_meta."
            "ALL => no limitations."
            "SMART (default) => restrictions on lazy evaluation types."
            "NONE => limit everything.",
        ).enum(ValueTrackingLevel)
        #### DOC END
