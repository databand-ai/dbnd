from __future__ import absolute_import

import pytest

from dbnd_examples.orchestration.dbnd_gcp.tool_dataflow import BeamWordCount


@pytest.mark.gcp
@pytest.mark.beam
@pytest.mark.skip(reason="Waiting for dataflow integration")
class TestGcpDataFlow(object):
    def test_beam_wordcount_cls_task(self):
        t = BeamWordCount(
            task_env="gcp",
            input="gs://dataflow-samples/shakespeare/kinglear.txt",
            task_version="now",
        )
        t.dbnd_run()

    def test_beam_wordcount_inline_task(self):
        from dbnd_examples.orchestration.dbnd_gcp.tool_dataflow import word_count

        word_count.dbnd_run(
            task_env="gcp",
            text_input="gs://dataflow-samples/shakespeare/kinglear.txt",
            task_version="now",
        )

    def test_beam_wordcount_inline_metrics(self):
        from dbnd_examples.orchestration.dbnd_gcp.tool_dataflow import (
            word_count_with_metrics,
        )

        word_count_with_metrics.dbnd_run(
            task_env="gcp",
            text_input="gs://dataflow-samples/shakespeare/kinglear.txt",
            task_version="now",
        )
