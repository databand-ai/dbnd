# © Copyright Databand.ai, an IBM Company 2022

from abc import ABC
from typing import Dict, List


class AdapterData:
    def __init__(self, data: object, failed: list[object], next_page: object):
        self.data = data
        self.failed = failed
        self.next_page = next_page


class Adapter(ABC):
    def get_last_cursor(self) -> object:
        raise NotImplementedError

    def get_data(
        self, cursor: object, batch_size: int, next_page: object
    ) -> AdapterData:
        raise NotImplementedError

    def update_data(self, to_update: List[object]) -> Dict[str, object]:
        raise NotImplementedError
