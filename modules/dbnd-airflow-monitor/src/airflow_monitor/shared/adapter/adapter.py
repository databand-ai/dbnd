# © Copyright Databand.ai, an IBM Company 2022
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

import attr

from dbnd._core.utils.timezone import utcnow


ASSET_TO_STATE_MAX_LIVENESS = timedelta(days=3)


class AssetState(Enum):
    """
    Enum representing the states of an asset.
    """

    INIT = "init"
    ACTIVE = "active"
    FINISHED = "finished"
    FAILED_REQUEST = "failed_request"
    MAX_RETRY = "max_retry"
    EXPIRED = "expired"

    @classmethod
    def get_active_states(cls) -> List[str]:
        """
        Get a list of active states.

        Returns:
            List[str]: A list of active states.
        """
        return [cls.INIT.value, cls.ACTIVE.value, cls.FAILED_REQUEST.value]


@attr.s(auto_attribs=True, frozen=True)
class AssetToState(ABC):
    """
    Represents the mapping of an asset to its state.
    """

    asset_id: str
    state: AssetState = AssetState.INIT
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    retry_count: int = 0

    def asdict(self) -> Dict[str, object]:
        """
        Convert the AssetToState object to a dictionary.

        Returns:
            dict: A dictionary representing the AssetToState object.
        """
        return {
            "asset_uri": self.asset_id,
            "state": self.state.value,
            "data": {"retry_count": self.retry_count},
        }

    @property
    def time_active(self) -> Optional[timedelta]:
        if not self.created_at:
            return

        return utcnow() - self.created_at

    @classmethod
    def from_dict(cls, assset_dict: Dict[str, object]) -> "AssetToState":
        """
        Create an AssetToState object from a dictionary.

        Args:
            asset_dict (dict): A dictionary representing an AssetToState object.

        Returns:
            AssetToState: An AssetToState object created from the dictionary.
        """
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


def update_asset_retry_state(
    asset_to_state: AssetToState, max_retries: int
) -> AssetToState:
    if asset_to_state.state == AssetState.ACTIVE:
        if (
            asset_to_state.time_active
            and asset_to_state.time_active > ASSET_TO_STATE_MAX_LIVENESS
        ):
            # Modify state to time out in case the asset was active more than 3 days
            return attr.evolve(asset_to_state, state=AssetState.EXPIRED)

        if asset_to_state.retry_count > 0:
            # reset retry counter if asset was FAILED (retry_count > 0) and now it's ACTIVE
            # the check for `retry_count > 0` is small performance optimization - to skip
            # evolve if nothing need to be changed, assuming most of the assets will be
            # ACTIVE with retry_count=0 - it can be significant.
            return attr.evolve(asset_to_state, retry_count=0)

    if asset_to_state.state == AssetState.FAILED_REQUEST:
        if asset_to_state.retry_count > max_retries:
            return attr.evolve(asset_to_state, state=AssetState.MAX_RETRY)

        return attr.evolve(asset_to_state, retry_count=asset_to_state.retry_count + 1)

    return asset_to_state


def update_assets_retry_state(
    assets_to_state: List[AssetToState], max_retries: int
) -> List[AssetToState]:
    """
    Process the given assets and their states based on the defined rules.

    Args:
        assets_to_state (List[AssetToState]): A list of AssetToState objects to be processed.

    Returns:
        List[AssetToState]: A list of processed AssetToState objects.
    """
    new_assets_to_state = [
        update_asset_retry_state(asset_to_state, max_retries)
        for asset_to_state in assets_to_state
    ]

    return new_assets_to_state


@attr.s(auto_attribs=True, frozen=True)
class Assets:
    """
    Represents a collection of assets and their states.
    """

    data: Any = None
    assets_to_state: Optional[List[AssetToState]] = None


@attr.s(auto_attribs=True, frozen=True)
class ThirdPartyInfo:
    metadata: Optional[Dict[str, str]]
    error_list: Optional[List[str]]


T = TypeVar("T")


class Adapter(ABC, Generic[T]):
    """
    Abstract base class for integration adapters.
    Subclasses should implement the methods to interact with a specific integration.
    seealso:: :class:`MockAdapter`
    seealso:: :class:`DataStageAdapter`
    seealso:: :class:`DbtAdapter`
    """

    @abstractmethod
    def init_cursor(self) -> T:
        """
        This method should be implemented by subclasses to initialize and return a cursor object.

        Returns:
            object: initial cursor object to query integration assets
        """
        raise NotImplementedError()

    @abstractmethod
    def get_new_assets_for_cursor(self, cursor: T) -> Tuple[Assets, T]:
        """
        Returns new assets and new cursor given current cursor.

        This method should be implemented by subclasses to return new assets available
        in the integration and corresponding new cursor object given initial cursor.

        This method should not do any exception handling, so that errors such as
        wrong credentials or missing resource will be raised and managed by the monitor.

        Args:
            cursor (object): The cursor object that will be used to query for integration assets

        Returns: Tuple[Assets, object]: A tuple containing assets and a next cursor
            example: Assets(data=None, assets_to_state={run_id: state.Init}), new_cursor
        """

        raise NotImplementedError()

    @abstractmethod
    def get_assets_data(self, assets: Assets) -> Assets:
        """
        Retrieve integration assets data for the given assets state mapping.

        This method should be implemented by subclasses to retrieve integration data for the given assets.

        Args:
            assets (Assets): represents the state and data of each asset batch

        Returns:
            Assets: a batch of integration raw data and state of each data object
        """
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
