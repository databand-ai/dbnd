# © Copyright Databand.ai, an IBM Company 2022

from abc import ABC
from typing import Dict, List


class Adapter(ABC):
    def get_last_cursor(self) -> object:
        raise NotImplementedError

    def get_data(
        self, cursor: object, batch_size: int, next_page: object
    ) -> (Dict[str, object], List[str], str):
        raise NotImplementedError

    def update_data(self, to_update: List[object]) -> Dict[str, object]:
        raise NotImplementedError
