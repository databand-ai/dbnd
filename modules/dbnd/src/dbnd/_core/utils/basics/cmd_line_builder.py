import subprocess


class CmdLineBuilder(object):
    def __init__(self):
        self._cmd_line = []

    def add(self, *args):
        self._cmd_line.extend(args)

    def extend(self, args):
        self._cmd_line.extend(args)

    def option(self, key, value):
        if value is None:
            return
        self.add(str(key), str(value))

    def option_bool(self, key, value):
        if value:
            self.add(str(key))

    def option_dict(self, key, value):
        pass

    def get_cmd(self):
        return list(self._cmd_line)

    def get_cmd_line(self):
        return subprocess.list2cmdline(self._cmd_line)
