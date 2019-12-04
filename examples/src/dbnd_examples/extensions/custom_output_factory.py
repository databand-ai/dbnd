from dbnd import ParameterDefinition, PythonTask, output, parameter


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
