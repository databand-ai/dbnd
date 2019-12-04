from __future__ import absolute_import

import pytest

from dbnd import parameter
from dbnd._core.task_build.task_context import current_task
from dbnd_examples.dbnd_gcp.tool_dataflow.dataflow_word_count import BeamWordCount
from dbnd_gcp.apache_beam.apache_beam_task import beam_task
from dbnd_gcp.apache_beam.parameters import beam_output
from targets import target
from targets.types import PathStr


@pytest.mark.beam
class TestLocalApacheBeam(object):
    def test_beam_wordcount_cls_task(self):
        t = BeamWordCount(input=__file__)
        t.dbnd_run()

    def test_beam_wordcount_inline_task(self):
        from dbnd_examples.dbnd_gcp.tool_dataflow.dataflow_word_count_inline import (
            word_count,
        )

        word_count.dbnd_run(text_input=__file__)

    def test_beam_wordcount_inline_metrics(self):
        from dbnd_examples.dbnd_gcp.tool_dataflow.dataflow_word_count_inline import (
            word_count_with_metrics,
        )

        word_count_with_metrics.dbnd_run(text_input=__file__)

    def test_beam_inline_options_object(self):
        import apache_beam as beam
        from apache_beam.io import ReadFromText, WriteToText
        from apache_beam.options.pipeline_options import PipelineOptions

        @beam_task(dataflow_build_pipeline=False, dataflow_wait_until_finish=False)
        def simple_beam(text_input=parameter[PathStr], text_output=beam_output):
            class RequireSomeParameter(PipelineOptions):
                @classmethod
                def _add_argparse_args(cls, parser):
                    parser.add_argument("--param", dest="param", required=True)

            dataflow_pipeline = current_task().build_pipeline(["--param", "2"])

            # Read the text file[pattern] into a PCollection.
            lines = dataflow_pipeline | "read" >> ReadFromText(text_input)

            x = dataflow_pipeline._options.view_as(RequireSomeParameter)
            assert x.param == "2"

            counts = (
                lines
                | "pair_with_one" >> beam.Map(lambda x: (x, 1))
                | "group" >> beam.GroupByKey()
            )

            counts | "write" >> WriteToText(text_output)
            target(text_output).mkdir()

            result = dataflow_pipeline.run()
            result.wait_until_finish()

        simple_beam.dbnd_run(text_input=__file__)
