---
"title": "Custom Target"
---
The goal of this example is to show how users can extend DBND and create custom targets. This example creates a target which is a directory with two files.

Define a custom target:

<!-- xfail -->
```python
from dbnd import parameter
from targets import DirTarget
from targets.values import TargetValueType


class MyCustomFolder(DirTarget):
    def __init__(self, *args, **kwargs):
        super(MyCustomFolder, self).__init__(*args, **kwargs)
        self.scores = self.partition("scores", config=file.csv)
        self.reports = self.partition("reports", config=file.csv)


my_custom_target = parameter.type(TargetValueType).custom_target(
    MyCustomFolder, folder=True
)
```

A pipeline to demonstrate how we read and write such targets

<!-- xfail -->
```python
from dbnd import PythonTask, output, PipelineTask

class TaskWithCustomOutput(PythonTask):
    custom = my_custom_target.output

    def run(self):
        self.custom.scores.write("1")
        self.custom.reports.write("2")
        self.custom.mark_success()


class TaskWithCustomInput(PythonTask):
    custom = my_custom_target
    t_output = output.data

    def run(self):
        assert self.custom.scores.read() == "1"
        assert self.custom.reports.read() == "2"
        self.t_output.write("done")


class CustomIOPipeline(PipelineTask):
    t_output = output.data

    def band(self):
        custom = TaskWithCustomOutput().custom
        self.t_output = TaskWithCustomInput(custom=custom).t_output
```
