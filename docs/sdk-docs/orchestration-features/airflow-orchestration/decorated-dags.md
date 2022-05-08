---
"title": "Native Airflow Execution"
---
This feature creates a bridge between DBND's implementation of pipelines and pre-existing Airflow DAGs. On an implementation level, it provides an explicit way of declaratively passing task messages (arguments) between Airflow operators. This approach was inspired by [AIP-31](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=148638736). The implementation uses XCom communication and XCom templates to transfer task messages.

Fully tested on Airflow 1.10.X.

## Code Example
Here is an example of how you achieve your goal:

```python
import logging

from datetime import datetime, timedelta
from typing import Tuple

import airflow

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from dbnd import task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# support airflow 1.10.0
if airflow.version.version == "1.10.0":
    class PythonOperator_airflow_1_10_0(PythonOperator):
        template_fields = ("templates_dict", "op_kwargs")

    PythonOperator = PythonOperator_airflow_1_10_0

@task
def calculate_alpha(alpha: int):
    logging.info("I am running")
    return alpha

@task
def prepare_data(data: str) -> Tuple[str,str]:
    return data, data

def read_and_write(input_path, output_path):
    logging.error("I am running")
    input_value = open(input_path, "r").read()
    with open(output_path, "w") as fp:
        fp.write(input_value)
        fp.write("\n\n")
        fp.write(str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S")))
    return "success"

with DAG(dag_id="dbnd_operators", default_args=default_args) as dag_operators:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = calculate_alpha(2)
    t2, t3 = prepare_data(t1)
    tp = PythonOperator(
        task_id="some_python_function",
        python_callable=read_and_write,
        op_kwargs={"input_path": t3, "output_path": "/tmp/output.txt"},
    )
    tp.set_upstream(t3.op)

    t1_op = t1.op
```

As you can see, the messages are passed explicitly between all three tasks:
- `t1`, the result of the first task is passed to the next task `prepare_data`
- `t2` and `t3` represent the results of `prepare_data`
- `read_and_write` is wrapped with an operator
- the new Python operator is defined as dependent upon `t3`'s execution (downstream) - explicitly


>⚠️ Note
> If you run a function marked with the `@task` decorator without a DAG context and without using the DBND library to run it, it will be executed as usual!

>📘 Note
> If you want to use << or >> to link a function marked with `@task` with non-functional operators (i.e. BashOperator), place the function or its result on the left side of the operator: my_task() >> BashOperator(). Otherwise, an error will be raised.

Using this method to pass arguments between tasks improves the user experience and also enables pipeline execution support for many use cases.

## Using dbnd_config
Let's look at the same example, but change the `default_args` defined at the very top:

```python
from datetime import timedelta
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'dbnd_config': {
        "calculate_alpha.alpha": 0.4
    }
}
```

Here a new key-value pair to the arguments called `dbnd_config` was added.

`dbnd_config` is expected to define a dictionary of configuration settings that you can pass to your tasks. For example, the `dbnd_config` in this code section defines that the `int` parameter `p_int_with_default` passed to `my_task` will be overridden and changed to `4` from the default value `0`.

To see the further possibilities of changing configuration settings, see [Object Configuration](doc:object-configuration).

## Jinja Templating
Airflow uses Jinja templates to pass macros to tasks. Jinja templates also work natively in DBND Airflow operator. To prevent errors, it is also possible to disable Jinja templates in DBND. Let's look at the following example:

```python
from dbnd import pipeline
from airflow import DAG

@pipeline
def current_date(p_date=None):
    return p_date

with DAG(dag_id="current_date_dag", default_args=default_args) as dag:
    current_date(p_date="{{ ts }}")
```

>📘 Note
> The example passes Jinja template to the `@pipeline` method, but it also works for decorated tasks `@task`.

The Airflow macro `{{ ts }}` marks the timestamp for execution. If we wanted to pass the actual literal argument `{{ ts }}`, we would have to disable Jinja templating for this parameter, as demonstrated:

```python
from airflow import DAG
from dbnd import parameter, pipeline

@pipeline
def current_date(p_date=parameter[str].disable_jinja_templating):
    return p_date

with DAG(dag_id="current_date_dag", default_args=default_args) as dag:
    current_date(p_date="{{ ts }}")
```

##Prioritization of Task Execution Order
To set different priorities for each task, you can utilize the Airflow's `weight_priority` property in `BaseOperator`.

Airflow supports this natively, so you are simply forwarding the `kwargs` over to the actual Airflow operator object. All you need is to pass the `task_airflow_op_kwargs` dictionary to the constructor of the DBND task, and then set a relevant value for the `priority_weight` key.

You can use the `task_airflow_op_kwargs`  to pass any BaseOperator parameters, such as `pool`, `execution_timeout`, `task_id`, etc.

See the following example:

```python
from airflow import DAG
airflow_op_kwargs = {"priority_weight": 50}
# Define DAG context
with DAG(dag_id="dbnd_operators", default_args=default_args) as dag_operators:
    t1 = calculate_alpha(2, task_airflow_op_kwargs=airflow_op_kwargs)
```

## Customizing Airflow Operator configuration
 Use Task.task_airflow_op_kwargs to pass defaults to the Airflow operator that would run this task