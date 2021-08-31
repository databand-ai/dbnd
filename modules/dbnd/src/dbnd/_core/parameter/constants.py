import enum


class ParameterScope(enum.Enum):
    task = "task"  # do not propagate value
    children = "children"  # propagate value to direct children
