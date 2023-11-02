# © Copyright Databand.ai, an IBM Company 2022

from typing import Dict, List, Optional

from airflow_monitor.shared.adapter.adapter import Adapter, Assets, ThirdPartyInfo


class MockAirflowAdapter(Adapter):
    def __init__(self):
        super(MockAirflowAdapter, self).__init__()
        self.metadata = None
        self.error_list = []

    def init_cursor(self) -> object:
        raise NotImplementedError()

    def init_assets_for_cursor(self, cursor: object) -> Assets:
        raise NotImplementedError()

    def get_assets_data(self, to_update: List[object]) -> Dict[str, object]:
        raise NotImplementedError()

    def get_third_party_info(self) -> Optional[ThirdPartyInfo]:
        return ThirdPartyInfo(metadata=self.metadata, error_list=self.error_list)
