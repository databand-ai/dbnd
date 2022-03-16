---
"title": "Task Class"
---
There are two main ways of defining tasks and pipelines with DBND. You can use @task and @pipeline decorators. You can also define tasks by creating a new task definition.  This approach is similar to what you can find in Python data classes, SQLAlchemy models, `marshmallow`, and many other famous Python frameworks.

The same class from the main page example can be defined as a class:
```python
class PrepareData(Task):
    data = parameter.data

    prepared_data = output.csv.data

    def run(self):
        self.prepared_data = do_something_with_data(self.data)
```

Parameters are the DBND's equivalent to creation of a constructor for each task. DBND requires you to declare these parameters in class scope using the `parameter` factory.  By doing this, DBND can take care of all the boilerplate code that would normally be needed in the constructor. Internally, the PrepareData object can now be constructed by running `PrepareData(data=your_data_frame)` or just `PrepareData()`. 

DBND also creates a command line parser that automatically handles the conversion of strings to Python types. This way you can invoke the job on the command line eg. by passing `--set data=DATAFRAME`.

**_NOTE:_**  You can also use @task to set the Task Class.

```python
from dbnd import task

@task
class PrepareData():
    data = parameter.data

    prepared_data = output.csv.data

    def run(self):
        self.prepared_data = do_something_with_data(self.data)
```

:warning: **If you are using Task Class**: we do not support the new super() syntax (PEP3135), keep using the old style: `super(PrepareData, self)`

## Pipeline
Using a similar approach, you can define the pipeline task as well.

```python
class PrepareDataPipeline(PipelineTask):
    data = parameter.data

    prepared_data = output.csv.data

    def band(self):
        self.prepared_data = PrepareData(data=self.data).prepared_data
```
> Please note that Pipeline task overrides `band` function instead of `run`.