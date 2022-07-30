# © Copyright Databand.ai, an IBM Company 2022

"""#### DOC START
from dbnd import output, parameter
from dbnd_spark import SparkConfig, SparkTask


class WordCountTask(SparkTask):
    text = parameter.data
    counters = output

    main_class = WORD_COUNT_MAIN_CLASS
    # overides value of SparkConfig object
    defaults = {SparkConfig.driver_memory: "2.5g"}

    def application_args(self):
        return [self.text, self.counters]


#### DOC END"""
