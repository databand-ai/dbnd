# © Copyright Databand.ai, an IBM Company 2022

import os

import attr

from dbnd.utils.api_client import ApiClient


DEFAULT_REQUEST_TIMEOUT = 30  # Seconds


@attr.s(auto_attribs=True)
class TrackingServiceConfig:
    url: str
    access_token: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "TrackingServiceConfig":
        config = cls(
            url=os.getenv("DBND__CORE__DATABAND_URL"),
            access_token=os.getenv("DBND__CORE__DATABAND_ACCESS_TOKEN"),
            user=os.getenv("DBND__CORE__DBND_USER", "databand"),
            password=os.getenv("DBND__CORE__DBND_PASSWORD", "databand"),
        )
        return config


def _get_api_client() -> ApiClient:
    tracking_service_config = TrackingServiceConfig.from_env()

    if tracking_service_config.access_token:
        credentials = {"token": tracking_service_config.access_token}
    else:
        # TODO: this is used by dbt/datastage monitors, should be deprecated!!
        credentials = {
            "username": tracking_service_config.user,
            "password": tracking_service_config.password,
        }

    return ApiClient(
        tracking_service_config.url,
        credentials=credentials,
        default_request_timeout=DEFAULT_REQUEST_TIMEOUT,
        default_retry_sleep=1,
        default_max_retry=3,
    )
