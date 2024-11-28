# © Copyright Databand.ai, an IBM Company 2022

from typing import List, Optional

import attr


@attr.s
class DbndDagRunsResponse:
    dag_run_ids = attr.ib()  # type: List[int]
    last_seen_dag_run_id = attr.ib()  # type: Optional[int]

    @classmethod
    def from_dict(cls, response):
        return cls(
            dag_run_ids=response["dag_run_ids"],
            last_seen_dag_run_id=response.get("last_seen_dag_run_id"),
        )
