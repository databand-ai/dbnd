from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import Config


class DbndAirflowOperatorConfig(Config):
    _conf__task_family = "airflow_operator"
    validate_operator_inputs = parameter(default=False).help(
        "Should dbnd airflow operator validate task inputs " "running in native airflow"
    )[bool]

    validate_operator_outputs = parameter(default=False).help(
        "Should dbnd airflow operator validate task "
        "outputs "
        "running in native airflow"
    )[bool]
