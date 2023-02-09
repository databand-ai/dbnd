# © Copyright Databand.ai, an IBM Company 2022

from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import attr


@attr.s(auto_attribs=True)
class AdapterData:
    data: object
    failed: List[object]
    next_page: object


@attr.s(auto_attribs=True)
class ThirdPartyInfo:
    metadata: Optional[Dict[str, str]]
    error_list: Optional[List[str]]


class Adapter(ABC):
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def get_last_cursor(self) -> object:
        raise NotImplementedError()

    @abstractmethod
    def get_new_data(
        self, cursor: object, batch_size: int, next_page: object
    ) -> AdapterData:
        raise NotImplementedError()

    @abstractmethod
    def get_update_data(self, to_update: List[object]) -> Dict[str, object]:
        raise NotImplementedError()

    def get_third_party_info(self) -> Optional[ThirdPartyInfo]:
        """
        Return information about the synced third party environment.

        metadata: information such as the version.
        Example: {a_version: 1.0, b_version: 2.5}

        error_list: list of errors that affect the integration with Databand.
        Example: packages or configurations that are missing and required for the integration.
        "
        """
        return None
