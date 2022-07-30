# © Copyright Databand.ai, an IBM Company 2022

import itertools
import logging

import tensorflow as tf

from databand.parameters import TargetParameter
from dbnd import PipelineTask, PythonTask, data, output, parameter
from dbnd_examples.data import data_repo


logger = logging.getLogger(__name__)


# Dummy Class to return
class Dummy:
    pass


# language=bash
example1 = """
# run this task
dbnd run -m dbnd_examples.tf_examples DataStats --DataStats-raw-data data/wine_quality.csv
"""


class DataStats(PythonTask):
    raw_data = data.target
    statistics = output.json

    def _validate_input(self, stat):
        # sample check, add your logic here
        if stat.iloc[0].max() != 4898:
            raise Exception("Failed to validate")

    def run(self):
        df = self.raw_data.read_df()
        #  this is just an example, your logic goes here
        stat = df.describe()
        self._validate_input(stat)
        stat.to_target(self.statistics)


# language=bash
example2 = """
# run this task separetly
dbnd run -m dbnd_examples.example_tensorflow TransformToTfRecords --TransformToTfRecords-raw-data data/wine_quality.csv
"""


class TransformToTfRecords(PythonTask):
    raw_data = data.target
    tf_records = output

    def run(self):
        # this is just an example, your transformation goes here
        input_data = self.raw_data.read_df().values
        with tf.io.TFRecordWriter(self.tf_records.path) as writer:
            for row in input_data:
                example = tf.train.Example(
                    features=tf.train.Features(
                        feature={
                            "x": tf.train.Feature(
                                float_list=tf.train.FloatList(value=row)
                            )
                        }
                    )
                )
                writer.write(example.SerializeToString())


# This class is just a placeholder to configure one instance of model training. For now, it just manage params
# per what we discussed.
class TrainTfModel(PythonTask):
    data = data

    block1 = parameter(description="class name for block 1")[str]
    block2 = parameter(description="class name for block 2")[str]
    block3 = parameter(description="class name for block 3")[str]

    precision = parameter[int]
    model = output

    def run(self):
        # Example: get blocks of tensorflow plan by class name
        b1 = self._get_block_by_name(self.block1)
        b2 = self._get_block_by_name(self.block2)
        b3 = self._get_block_by_name(self.block3)
        logger.info("b1=%s b2=%s b3=%s", b1, b2, b3)
        # TODO here tensorflow graph should be constructed and executed. The output would be a model, saved as a file,
        # next line is just a demo
        self.model.write_pickle(self)

    def _get_block_by_name(self, class_name):
        # module = importlib.import_module(self.module_name)
        # class_ = getattr(module, class_name)
        # for now, return dummy class
        class_ = globals()["Dummy"]
        return class_()


class GenerateReport(PythonTask):
    model = data
    report = output

    def run(self):
        # for now, just a stub
        logger.log("This is a report for %s" % self.model)

        self.report.write("Some report!")


# this is to show how you can program multiple experiments and run them
# in the future you would be able to run them in parallel
class Experiment(PipelineTask):
    data = data(data_repo.wines)
    model = output
    validation_stats = output

    block1 = parameter.value(default="a")
    block2 = parameter.value(default="b")
    block3 = parameter.value(default="c")

    precision = parameter[int].default(32)

    def band(self):
        self.validation_stats = DataStats(raw_data=self.data).statistics
        transformed_data = TransformToTfRecords(raw_data=self.data)
        # transformed_data.set_upstream(validation_stats)

        self.model = TrainTfModel(
            data=transformed_data.tf_records,
            block1=self.block1,
            block2=self.block2,
            block3=self.block3,
            precision=self.precision,
        ).model


class ParameterSearch(PipelineTask):
    data = TargetParameter.data(default=data_repo.wines)
    model = output

    block1_list = ["a1", "a2", "a3"]
    block2_list = ["b1", "b2", "b3"]
    block3_list = ["c1", "c2", "c3"]

    def band(self):
        models = []
        for a, b, c in itertools.product(
            self.block1_list, self.block2_list, self.block3_list
        ):
            exp = Experiment(data=self.data, block1=a, block2=b, block3=c, precision=64)
            models[exp.task_id] = exp
        self.model = models
