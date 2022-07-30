# © Copyright Databand.ai, an IBM Company 2022

import attr

from dbnd._core.configuration import environ_config
from dbnd._core.utils.basics.environ_utils import environ_enabled


def disable_airflow_subdag_tracking():
    return environ_enabled(
        environ_config.ENV_DBND__DISABLE_AIRFLOW_SUBDAG_TRACKING, False
    )


@attr.s
class AirflowTaskContext(object):
    dag_id = attr.ib()  # type: str
    execution_date = attr.ib()  # type: str
    task_id = attr.ib()  # type: str
    try_number = attr.ib(default=1)
    context = attr.ib(default=None)  # type: Optional[dict]
    airflow_instance_uid = attr.ib(default=None)  # type: Optional[UUID]

    root_dag_id = attr.ib(init=False, repr=False)  # type: str
    is_subdag = attr.ib(default=None, repr=False)  # type: bool

    def __attrs_post_init__(self):
        if self.is_subdag is None:
            self.is_subdag = (
                "." in self.dag_id
            ) and not disable_airflow_subdag_tracking()
        if self.is_subdag:
            dag_breadcrumb = self.dag_id.split(".", 1)
            self.root_dag_id = dag_breadcrumb[0]
            self.parent_dags = []
            # for subdag "root_dag.subdag1.subdag2.subdag3" it should return
            # root_dag, root_dag.subdag1, root_dag.subdag1.subdag2
            # WITHOUT the dag_id itself!
            cur_name = []
            for name_part in dag_breadcrumb[:-1]:
                cur_name.append(name_part)
                self.parent_dags.append(".".join(cur_name))
        else:
            self.root_dag_id = self.dag_id
            self.parent_dags = []
