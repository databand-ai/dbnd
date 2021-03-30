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
