# © Copyright Databand.ai, an IBM Company 2022

from typing import Dict, List, Optional

from airflow_monitor.shared.adapter.adapter import Adapter, AdapterData, ThirdPartyInfo


class MockAirflowAdapter(Adapter):
    def __init__(self, config):
        super(MockAirflowAdapter, self).__init__(config)
        self.metadata = None
        self.error_list = []

    def get_last_cursor(self) -> object:
        raise NotImplementedError()

    def get_new_data(
        self, cursor: object, batch_size: int, next_page: object
    ) -> AdapterData:
        raise NotImplementedError()

    def get_update_data(self, to_update: List[object]) -> Dict[str, object]:
        raise NotImplementedError()

    def get_third_party_info(self) -> Optional[ThirdPartyInfo]:
        return ThirdPartyInfo(metadata=self.metadata, error_list=self.error_list)
