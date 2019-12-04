from __future__ import absolute_import

import base64
import logging

from io import BytesIO

from dbnd._vendor import fast_hasher
from matplotlib import figure
from targets.config import get_value_preview_max_len
from targets.values.builtins_values import DataValueType


logger = logging.getLogger(__name__)


class MatplotlibFigureValueType(DataValueType):
    type = figure.Figure
    config_name = "matplotlib_figure"

    def to_signature(self, x):
        return fast_hasher.hash(x)

    def to_preview(self, value):
        try:
            figure_bytes = BytesIO()
            value.savefig(figure_bytes, format="png")
            figure_bytes.seek(0)
            preview = base64.b64encode(figure_bytes.read())
            if len(preview) > get_value_preview_max_len():
                return (
                    ".png size %s is bigger than %s bytes, change '[core/value_preview_max_len]'"
                    % (len(preview), get_value_preview_max_len())
                )
            return preview
        except Exception as ex:
            logger.warning(
                "Failed to calculate preview for matplotlib figure: %s", str(ex)
            )
            return None
