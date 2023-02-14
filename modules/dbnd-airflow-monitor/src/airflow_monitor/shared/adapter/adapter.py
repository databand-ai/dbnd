# © Copyright Databand.ai, an IBM Company 2022

from abc import ABC


class Adapter(ABC):
    def get_last_cursor(self) -> object:
        raise NotImplementedError

    def get_data(
        self, cursor: object, batch_size: int, next_page: object
    ) -> (dict[str, object], list[str], str):
        raise NotImplementedError

    def update_data(self, to_update: list[object]) -> dict[str, object]:
        raise NotImplementedError
