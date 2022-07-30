# © Copyright Databand.ai, an IBM Company 2022

from __future__ import absolute_import

import base64
import logging

from io import BytesIO

from matplotlib import figure

from dbnd._vendor import fast_hasher
from targets.values.builtins_values import DataValueType


logger = logging.getLogger(__name__)


class MatplotlibFigureValueType(DataValueType):
    type = figure.Figure
    config_name = "matplotlib_figure"

    def to_signature(self, x):
        return fast_hasher.hash(x)

    def to_preview(self, value, preview_size):
        try:
            figure_bytes = BytesIO()
            value.savefig(figure_bytes, format="png")
            figure_bytes.seek(0)
            preview = base64.b64encode(figure_bytes.read())
            if len(preview) > preview_size:
                return (
                    ".png size %s is bigger than %s bytes, change '[feature]/log_value_preview_max_len'"
                    % (len(preview), preview_size)
                )
            return preview
        except Exception as ex:
            logger.warning(
                "Failed to calculate preview for matplotlib figure: %s", str(ex)
            )
            return None
