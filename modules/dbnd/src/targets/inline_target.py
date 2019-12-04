from targets import InMemoryTarget


class InlineTarget(InMemoryTarget):
    def __init__(self, root_target, obj, value_type, **kwargs):
        super(InlineTarget, self).__init__(obj=obj, value_type=value_type, **kwargs)
        self.root_target = root_target
        self.value_metrics = root_target.value_metrics

    def __repr__(self):
        if self.root_target:
            return repr(self.root_target)
        return super(InlineTarget, self).__repr__()

    def __str__(self):
        if self.root_target:
            return str(self.root_target)
        return super(InlineTarget, self).__str__()
