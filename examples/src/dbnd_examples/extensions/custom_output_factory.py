import logging

import six

from dbnd import (
    ParameterDefinition,
    PipelineTask,
    PythonTask,
    output,
    parameter,
    pipeline,
)


def _custom_outputs_factory(
    task, task_output
):  # type: (PythonTask, ParameterDefinition) -> dict
    result = {}
    target = task_output.build_target(task)
    for i in range(task.parts):
        name = "part_%s" % i
        result[name] = (
            target.partition(name="train_%s" % name),
            target.partition(name="test_%s" % name),
        )

    return result


class DataSplitIntoMultipleOutputs(PythonTask):
    parts = parameter.value(3)
    splits = output.csv.folder(output_factory=_custom_outputs_factory)

    def run(self):
        for key, split in self.splits.items():
            train, test = split
            train.write(key)
            test.write(key)


def _get_all_splits(task, task_output):  # type: (Task, Any) -> Dict[str,Target]
    result = {}
    target = task_output.build_target(task)
    for i in range(task.splits_count):
        result[str(i)] = target.partition(name="split_%s" % i)
    return result


class DataSplit(PythonTask):
    splits_count = parameter.default(2)
    splits = output.csv.folder(output_factory=_get_all_splits)

    def run(self):
        for key, split in six.iteritems(self.splits):
            logging.info("writing split")
            split.write("split_%s" % key)


# we can implement using classes
# we will treat output as a Dict of str:result (result is Target, deferred data result)
# while running band it still doesnt' exist, as we are only building a graph (we are before exectuion)
class DataSplitBand(PipelineTask):
    split_selector = parameter[str]
    selected_split = output

    def band(self):
        logging.info(
            "All logs will be printed on task creation - before the real execution"
        )
        splits = DataSplit().splits
        for key, split in six.iteritems(splits):
            logging.info("split %s %s", key, split)

        self.selected_split = splits[self.split_selector]


# here we wrap the pipeline -- if we want to do that
class DataSplitUsage(PipelineTask):
    selected_split = output

    def band(self):
        self.selected_split = DataSplitBand(split_selector="0").selected_split


# and here the regular dbnd pipeline implementation,  notice, we are selecting part "1" this time
@pipeline
def data_split_selector():
    return DataSplit().splits["1"]
