# © Copyright Databand.ai, an IBM Company 2022
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Generator, List, Optional, Tuple

import attr


class AssetState(Enum):
    INIT = "init"
    ACTIVE = "active"
    FINISHED = "finished"
    FAILED_REQUEST = "failed_request"
    MAX_RETRY = "max_retry"

    @classmethod
    def get_active_states(cls):
        return [cls.INIT.value, cls.ACTIVE.value, cls.FAILED_REQUEST.value]


@attr.s(auto_attribs=True)
class AssetToState(ABC):
    asset_id: str
    state: AssetState
    retry_count: int = 0

    def asdict(self):
        return {
            "asset_uri": self.asset_id,
            "state": self.state.value,
            "data": {"retry_count": self.retry_count},
        }

    @classmethod
    def from_dict(cls, assset_dict) -> "AssetToState":
        asset_id = assset_dict.get("asset_uri")
        state = assset_dict.get("state")
        asset_state = None
        if asset_id is None or state is None:
            raise Exception(
                "active asset does not contain id or state, will be skipped"
            )
        try:
            asset_state = AssetState(state)
        except:
            raise Exception("asset state is unrecognized, will be skipped")
        return AssetToState(
            asset_id=asset_id,
            state=asset_state,
            retry_count=assset_dict.get("data", {}).get("retry_count", 0),
        )


@attr.s(auto_attribs=True)
class AssetsToStatesMachine:
    max_retries: int = 5

    def process(self, assets_to_state: List[AssetToState]) -> List[AssetToState]:
        new_assets_to_state = []
        for asset_to_state in assets_to_state:
            if asset_to_state.state == AssetState.FAILED_REQUEST:
                if asset_to_state.retry_count > self.max_retries:
                    asset_to_state.state = AssetState.MAX_RETRY
                else:
                    asset_to_state.retry_count += 1
            elif asset_to_state.state == AssetState.MAX_RETRY:
                continue
            new_assets_to_state.append(asset_to_state)
        return new_assets_to_state


@attr.s(auto_attribs=True)
class Assets:
    data: Optional[object] = None
    assets_to_state: Optional[List[AssetToState]] = None


@attr.s(auto_attribs=True)
class ThirdPartyInfo:
    metadata: Optional[Dict[str, str]]
    error_list: Optional[List[str]]


class Adapter(ABC):
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def init_cursor(self) -> object:
        raise NotImplementedError()

    @abstractmethod
    def init_assets_for_cursor(
        self, cursor: object, batch_size: int
    ) -> Generator[Tuple[Assets, str], None, None]:
        raise NotImplementedError()

    @abstractmethod
    def get_assets_data(self, assets: Assets, next_page: object) -> Assets:
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
