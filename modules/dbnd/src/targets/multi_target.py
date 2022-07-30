# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.utils.traversing import flatten
from targets.data_target import DataTarget
from targets.target_config import TargetConfig
from targets.utils.open_multiple import MultiTargetOpen


logger = logging.getLogger(__name__)


class MultiTarget(DataTarget):
    def __init__(self, targets, properties=None, source=None):
        super(MultiTarget, self).__init__(properties=properties, source=source)
        self._targets = targets

        if targets and len({t.config for t in targets if hasattr(t, "config")}) == 1:
            self.config = targets[0].config
        else:
            self.config = TargetConfig()

    @property
    def targets(self):
        return self._targets

    def open(self, mode="r"):
        """ """
        if "r" in mode:
            return MultiTargetOpen(targets=self.list_partitions(), mode=mode)
        elif "w" in mode:
            raise NotImplementedError(
                "Multi part target can be used only for reading! You can not remove it!"
            )
        else:
            raise ValueError("Unsupported open mode '{}'".format(mode))

    def remove(self):
        raise NotImplementedError(
            "Multi part target can be used only for reading! You can not remove it!"
        )

    def exists(self):
        return all((t.exists() for t in flatten(self.targets) if t is not None))

    def list_partitions(self):
        return self.targets

    def __repr__(self):
        return "target('%s')" % ",".join(map(str, self.targets))

    def __str__(self):
        return ",".join(map(str, self.targets))
