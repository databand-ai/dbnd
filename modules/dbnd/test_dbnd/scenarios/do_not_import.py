# do not import
# we use this file to check --module
from dbnd import Task, output, parameter


class DynamicImportTask(Task):
    x = parameter[int]
    o = output

    def run(self):
        assert self.x == 123
        self.o.write("done")
