class DataTargetCtrl(object):
    def __init__(self, target):
        """
        :param target: DataTarget
        """
        super(DataTargetCtrl, self).__init__()
        from targets.data_target import DataTarget

        self.target = target  # type: DataTarget
