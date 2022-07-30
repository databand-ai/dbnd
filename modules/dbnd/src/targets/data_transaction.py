# © Copyright Databand.ai, an IBM Company 2022

from targets import DataTarget


class DataTransaction(object):
    def __init__(self, target):  # type: (DataTarget) -> DataTransaction
        super(DataTransaction, self).__init__()
        self.target = target
        # right now we are not having "atomic writes"
        # just marking the target as complete at the end

    def __enter__(self):
        return self.target

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            # There were no exceptions
            self.target.mark_success()
        return False  # False means we don't suppress the exception
