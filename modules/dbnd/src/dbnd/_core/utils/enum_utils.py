# © Copyright Databand.ai, an IBM Company 2022


def enum_values(enum):
    return [getattr(enum, k) for k in dir(enum) if not k.startswith("__")]
