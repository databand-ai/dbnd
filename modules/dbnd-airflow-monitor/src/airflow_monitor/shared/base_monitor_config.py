# © Copyright Databand.ai, an IBM Company 2022
import os

from typing import Optional

import attr

from attr.converters import optional


@attr.s(auto_attribs=True)
class BaseMonitorConfig:
    _env_prefix = "DBND__MONITOR__"

    # Set which port will be used for prometheus.
    prometheus_port: int = attr.ib(default=8000, converter=int)

    # Set the sleep time, in seconds, between fetches, when the monitor is not busy.
    interval: int = attr.ib(default=5, converter=int)

    # Set a cap for the number of monitor iterations. This is optional.
    number_of_iterations: Optional[int] = attr.ib(default=None, converter=optional(int))

    # Set a cap for the number of seconds to run the monitor. This is optional.
    stop_after: Optional[int] = attr.ib(default=None, converter=optional(int))

    # Log format to use: text/json
    log_format: str = "text"

    # Sync only integration with this name
    syncer_name: Optional[str] = None

    @property
    def use_json_logging(self):
        return self.log_format == "json"

    @classmethod
    def from_env(cls, **overrides):
        env_prefix = cls._env_prefix

        for field in attr.fields(cls):
            if field.name in overrides:
                continue

            env_var_name = f"{env_prefix}{field.name}".upper()
            env_val = os.environ.get(env_var_name)
            if env_val is not None:
                overrides[field.name] = env_val

        return cls(**overrides)


NOTHING = object()


@attr.s(auto_attribs=True)
class BaseMonitorState:
    monitor_error_message: Optional[str] = NOTHING

    def as_dict(self) -> dict:
        return {k: v for k, v in attr.asdict(self).items() if v is not NOTHING}
