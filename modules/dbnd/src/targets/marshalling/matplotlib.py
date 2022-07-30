# © Copyright Databand.ai, an IBM Company 2022

from __future__ import absolute_import

import io

from matplotlib import figure

from targets.marshalling.marshaller import Marshaller
from targets.target_config import FileFormat


class MatplotlibFigureMarshaller(Marshaller):
    type = figure.Figure
    file_format = FileFormat.jpeg

    def value_to_target(self, value, target, **kwargs):
        buf = io.BytesIO()
        value.savefig(buf, **kwargs)

        with target.open("wb") as fd:
            fd.write(buf.getvalue())
