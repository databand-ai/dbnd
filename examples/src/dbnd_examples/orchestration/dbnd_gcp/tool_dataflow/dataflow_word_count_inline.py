#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file has been modified by databand.ai to for more advance example.

from __future__ import absolute_import

import logging
import re

import apache_beam as beam

from apache_beam.io import ReadFromText, WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from past.builtins import unicode

from dbnd import dbnd_main
from dbnd_gcp.apache_beam.apache_beam_task import beam_task
from dbnd_gcp.apache_beam.parameters import beam_output
from targets import target
from targets.types import PathStr


class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def __init__(self):
        self.words_counter = Metrics.counter(self.__class__, "words")
        self.word_lengths_counter = Metrics.counter(self.__class__, "word_lengths")
        self.word_lengths_dist = Metrics.distribution(self.__class__, "word_len_dist")
        self.empty_line_counter = Metrics.counter(self.__class__, "empty_lines")

    def process(self, element):
        """Returns an iterator over the words of this element.

        The element is a line of text.  If the line is blank, note that, too.

        Args:
          element: the element being processed

        Returns:
          The processed element.
        """
        text_line = element.strip()
        if not text_line:
            self.empty_line_counter.inc(1)
        words = re.findall(r"[\w\']+", text_line, re.UNICODE)
        for w in words:
            self.words_counter.inc()
            self.word_lengths_counter.inc(len(w))
            self.word_lengths_dist.update(len(w))
        return words


@beam_task
def word_count(dataflow_pipeline, text_input, text_output=beam_output):
    # type:(Any, PathStr, PathStr) -> None
    # Read the text file[pattern] into a PCollection.
    lines = dataflow_pipeline | "read" >> ReadFromText(text_input)

    # Count the occurrences of each word.
    def count_ones(word_ones):
        (word, ones) = word_ones
        return (word, sum(ones))

    counts = (
        lines
        | "split" >> (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
        | "pair_with_one" >> beam.Map(lambda x: (x, 1))
        | "group" >> beam.GroupByKey()
        | "count" >> beam.Map(count_ones)
    )

    # Format the counts into a PCollection of strings.
    def format_result(word_count):
        (word, count) = word_count
        return "%s: %d" % (word, count)

    output = counts | "format" >> beam.Map(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned

    output | "write" >> WriteToText(text_output)

    target(text_output).mkdir()


@beam_task(dataflow_wait_until_finish=False)
def word_count_with_metrics(text_input, dataflow_pipeline, text_output=beam_output):
    word_count(
        text_input=text_input,
        text_output=text_output,
        dataflow_pipeline=dataflow_pipeline,
    )
    result = dataflow_pipeline.run()
    result.wait_until_finish()

    # Do not query metrics when creating a template which doesn't run
    if (
        not hasattr(result, "has_job") or result.has_job  # direct runner
    ):  # not just a template creation
        empty_lines_filter = MetricsFilter().with_name("empty_lines")
        query_result = result.metrics().query(empty_lines_filter)
        if query_result["counters"]:
            empty_lines_counter = query_result["counters"][0]
            logging.info("number of empty lines: %d", empty_lines_counter.result)

        word_lengths_filter = MetricsFilter().with_name("word_len_dist")
        query_result = result.metrics().query(word_lengths_filter)
        if query_result["distributions"]:
            word_lengths_dist = query_result["distributions"][0]
            logging.info("average word length: %d", word_lengths_dist.result.mean)


if __name__ == "__main__":
    dbnd_main()
    # word_count.dbnd_run(
    #     # text_input="~/data/kinglear.txt",
    #     text_input="gs://dataflow-samples/shakespeare/kinglear.txt",
    #     task_env="gcp",
    #     task_version="now",
    # )
