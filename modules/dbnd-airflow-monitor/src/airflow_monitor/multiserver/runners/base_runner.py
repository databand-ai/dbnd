class BaseRunner(object):
    def __init__(self, target, **kwargs):
        self.target = target
        self.kwargs = kwargs

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def heartbeat(self):
        raise NotImplementedError()

    def is_alive(self):
        raise NotImplementedError()

    def __str__(self):
        s = ", ".join([f"{k}={v}" for k, v in self.kwargs.items()])
        return f"{self.__class__.__name__}({self.target.__name__}, {s})"
