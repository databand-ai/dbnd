# © Copyright Databand.ai, an IBM Company 2022

from typing import Any, Dict, Optional

from targets import FileTarget, target
from targets.errors import FileNotFoundException


class RunResultBand(object):
    """Wrap the access to the result of the run"""

    def __init__(self, paths):
        # type: (Dict[str, str])->RunResultBand
        self.paths = paths

    def load(self, name, value_type=None):
        # type: (str, Optional[str]) -> Any
        return target(self.paths[name]).load(value_type)

    def __iter__(self):
        return iter(self.paths)

    @classmethod
    def from_target(cls, results_map_target):
        # type: (FileTarget)->RunResultBand
        if results_map_target.exists():
            # we accepting the task_band of the pipeline:
            #   a json with a mapping between any result's name and its target
            paths = results_map_target.load(Dict[str, str])
            return cls(paths=paths)

        raise FileNotFoundException(
            "Couldn't access tasks results file at {location}.".format(
                location=results_map_target.path
            )
        )
