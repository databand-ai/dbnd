# © Copyright Databand.ai, an IBM Company 2022

import json
import os

from collections import Counter

import matplotlib

import dbnd

from dbnd import PipelineTask, data, output
from dbnd_test_scenarios import scenario_path


matplotlib.use("Agg")


class WordCount(dbnd.Task):
    text = data.target
    counters = output

    def run(self):
        result = Counter()
        chars_read = 0

        for line in self.text.readlines():
            result.update(line.split())
            chars_read += len(line)

        self.log_metric("chars_read", chars_read)
        self.log_artifact("figure.png", self.gen_hist(result))

        self.counters.write(json.dumps(result))

    def gen_hist(self, result):
        import matplotlib.pyplot as plt

        words, counts = zip(*result.most_common(5))
        plt.bar(words, counts)
        return plt.gcf()


class WordCountPipeline(PipelineTask):
    counter = output

    def band(self):
        count = WordCount(text=scenario_path("data/some_log.txt"))
        self.counter = count.counters


def test_word_count():
    t = WordCount(text=__file__)
    t.dbnd_run()

    meta_path = t.ctrl.last_task_run.attempt_folder
    chars_read = os.path.join(meta_path.path, "metrics", "user", "chars_read")
    artifact = os.path.join(meta_path.path, "artifacts", "figure.png")

    assert os.path.exists(chars_read)
    assert os.path.exists(artifact)
