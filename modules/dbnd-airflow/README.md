# Dbnd Airflow Operator

This plugin was written to provide an explicit way of declaratively passing messages between two airflow operators.

This plugin was inspired by [AIP-31](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-31%3A+Airflow+functional+DAG+definition).
Essentially, this plugin connects between dbnd's implementation of tasks and pipelines to airflow operators.

This implementation uses XCom communication and XCom templates to transfer said messages.
This plugin is fully functional, however as soon as AIP-31 is implemented it will support all edge-cases.

Fully tested on airflow 1.10.X.

# Code Example

Here is an example of how we achieve our goal:

```python
import logging
from typing import Tuple
from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from dbnd import task

# Define arguments that we will pass to our DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}
@task
def my_task(p_int=3, p_str="check", p_int_with_default=0) -> str:
    logging.info("I am running")
    return "success"


@task
def my_multiple_outputs(p_str="some_string") -> Tuple[int, str]:
    return (1, p_str + "_extra_postfix")


def some_python_function(input_path, output_path):
    logging.error("I am running")
    input_value = open(input_path, "r").read()
    with open(output_path, "w") as output_file:
        output_file.write(input_value)
        output_file.write("\n\n")
        output_file.write(str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S")))
    return "success"

# Define DAG context
with DAG(dag_id="dbnd_operators", default_args=default_args) as dag_operators:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # All tasks and operators created under this DAG context will be collected as a part of this DAG
    t1 = my_task(2)
    t2, t3 = my_multiple_outputs(t1)
    python_op = PythonOperator(
        task_id="some_python_function",
        python_callable=some_python_function,
        op_kwargs={"input_path": t3, "output_path": "/tmp/output.txt"},
    )
    """
    t3.op describes the operator used to execute my_multiple_outputs
    This call defines the some_python_function task's operator as dependent upon t3's operator
    """
    python_op.set_upstream(t3.op)
```

As you can see, messages are passed explicitly between all three tasks:

-   t1, the result of the first task is passed to the next task my_multiple_outputs
-   t2 and t3 represent the results of my_multiple_outputs
-   some_python_function is wrapped with an operator
-   The new python operator is defined as dependent upon t3's execution (downstream) - explicitly.

> Note: If you run a function marked with the `@task` decorator without a DAG context, and without using the dbnd
> library to run it - it will execute absolutely normally!

Using this method to pass arguments between tasks not only improves developer user-experience, but also allows
for pipeline execution support for many use-cases. It does not break currently existing DAGs.

# Using dbnd_config

Let's look at the example again, but change the default_args defined at the very top:

```python
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'dbnd_config': {
        "my_task.p_int_with_default": 4
    }
}
```

Added a new key-value pair to the arguments called `dbnd_config`

`dbnd_config` is expected to define a dictionary of configuration settings that you can pass to your tasks. For example,
the `dbnd_config` in this code section defines that the int parameter `p_int_with_default` passed to my_task will be
overridden and changed to `4` from the default value `0`.

To see further possibilities of changing configuration settings, see our [documentation](https://dbnd.readme.io/)
