# © Copyright Databand.ai, an IBM Company 2022

import enum


class ParameterScope(enum.Enum):
    """ParameterScope."""

    task = "task"  # do not propagate value
    children = "children"  # propagate value to direct children
