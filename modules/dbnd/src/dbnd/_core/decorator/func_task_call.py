import logging
import typing

from typing import Any, Dict, Tuple, Type

import attr
import six


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd._core.task.task import Task


@attr.s
class TaskCallState(object):
    started = attr.ib(default=False)
    finished = attr.ib(default=False)
    result_saved = attr.ib(default=False)

    result = attr.ib(default=None)

    should_store_result = attr.ib(default=False)

    def start(self):
        self.started = True
        self.finished = False
        self.result = None

    def finish(self, result=None):
        self.finished = True
        if self.should_store_result:
            self.result_saved = True
            self.result = result


CALL_FAILURE_OBJ = object()


@attr.s
class FuncCall(object):
    task_cls = attr.ib()  # type: Type[Task]
    call_args = attr.ib()  # type:  Tuple[Any]
    call_kwargs = attr.ib()  # type:  Dict[str,Any]
    call_user_code = attr.ib()

    def invoke(self):
        # make sure to invoke only with original decorator spec
        spec_args = self.task_cls._conf__decorator_spec.args
        kwargs = {
            key: value
            for key, value in six.iteritems(self.call_kwargs)
            if key in spec_args
        }

        func = self.call_user_code
        return func(*self.call_args, **kwargs)
