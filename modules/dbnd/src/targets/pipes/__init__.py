import os

import six

from targets.pipes.base import IOPipeline, NopPipeline
from targets.pipes.bzip2 import Bzip2Pipeline
from targets.pipes.gzip import GzipPipeline
from targets.pipes.text import MixedUnicodeBytesFormat, NewlinePipeline, TextPipeline


Nop = NopPipeline()
Text = TextPipeline()
UTF8 = TextPipeline(encoding="utf8")
SysNewLine = NewlinePipeline()
Gzip = GzipPipeline()
Bzip2 = Bzip2Pipeline()
MixedUnicodeBytes = MixedUnicodeBytesFormat()

SeamlessFilters = [Nop, Text]


def is_seamless_pipe(target_filter):
    return not target_filter or target_filter in SeamlessFilters
