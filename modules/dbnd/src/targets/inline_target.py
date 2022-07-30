# © Copyright Databand.ai, an IBM Company 2022

from targets import InMemoryTarget


class InlineTarget(InMemoryTarget):
    """
    represents in-place value provided by user,
    used by inline functions calls to track data dependencies
    """

    def __init__(self, root_target, obj, value_type, **kwargs):
        super(InlineTarget, self).__init__(obj=obj, value_type=value_type, **kwargs)
        self.root_target = root_target
        self.target_meta = root_target.target_meta

    def __repr__(self):
        if self.root_target:
            return repr(self.root_target)
        return super(InlineTarget, self).__repr__()

    def __str__(self):
        if self.root_target:
            return str(self.root_target)
        return super(InlineTarget, self).__str__()
