# © Copyright Databand.ai, an IBM Company 2022

from datetime import datetime

import luigi

from tests.luigi_examples.top_artists import LuigiTestException


"""
This pipeline has an interesting edge case - both TaskA and TaskB output multiple targets, as well as manage them
in dictionaries, as well as they both use the same key names.
"""


class TaskA(luigi.Task):
    version = luigi.Parameter(default=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

    def requires(self):
        return []

    def output(self):
        return {
            "output1": luigi.LocalTarget("/tmp/task_a_out1" + str(self.version)),
            "output2": luigi.LocalTarget("/tmp/task_a_out2" + str(self.version)),
        }

    def run(self):
        with self.output()["output1"].open("w") as outfile:
            outfile.write("foo\n")
        with self.output()["output2"].open("w") as outfile:
            outfile.write("foo\n")


class TaskB(luigi.Task):
    version = luigi.Parameter(default=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

    def requires(self):
        return []

    def output(self):
        return {
            "output1": luigi.LocalTarget("/tmp/task_b_out1" + str(self.version)),
            "output2": luigi.LocalTarget("/tmp/task_b_out2" + str(self.version)),
        }

    def run(self):
        with self.output()["output1"].open("w") as outfile:
            outfile.write("bar\n")
        with self.output()["output2"].open("w") as outfile:
            outfile.write("bar\n")


class TaskC(luigi.Task):
    version = luigi.Parameter(default=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

    def requires(self):
        return {"input_a": TaskA(), "input_b": TaskB()}

    def output(self):
        return luigi.LocalTarget(
            self.input()["input_a"]["output1"].path + self.version + ".task_c"
        )

    def run(self):
        # we need to know the name of TaskA's output
        with self.input()["input_a"]["output1"].open() as infile_a1:
            # ... and same here ...
            with self.input()["input_a"]["output2"].open() as infile_a2:
                # we need to know the name of TaskB's output
                with self.input()["input_b"]["output1"].open() as infile_b1:
                    # ... and same here ...
                    with self.input()["input_b"]["output2"].open() as infile_b2:
                        with self.output().open("w") as outfile:
                            for line in infile_a1:
                                outfile.write(line)
                            for line in infile_a2:
                                outfile.write(line)
                            for line in infile_b1:
                                outfile.write(line)
                            for line in infile_b2:
                                outfile.write(line)


class MyWrapperTask(luigi.WrapperTask):
    version = luigi.Parameter(default=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

    def requires(self):
        return [TaskC()]

    def run(self):
        with self.output().open("w") as out_file:
            out_file.write("All wrapped up!")

    def output(self):
        return luigi.LocalTarget("/tmp/data/wrapper_{}.txt".format(self.version))


class MyWrapperTaskRunFail(luigi.WrapperTask):
    version = luigi.Parameter(default=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

    def requires(self):
        return [TaskB()]

    def run(self):
        raise LuigiTestException("Wrapper task run raises test exception!")

    def output(self):
        return luigi.LocalTarget(
            "/tmp/data/wrapper_run_fail_{}.txt".format(self.version)
        )


class MyWrapperTaskRequiresFail(MyWrapperTask):
    def requires(self):
        raise LuigiTestException("Wrapper task requires raises test exception!")

    def output(self):
        return luigi.LocalTarget(
            "/tmp/data/wrapper_req_fail_{}.txt".format(self.version)
        )


class MyWrapperTaskOutputFails(MyWrapperTask):
    def output(self):
        raise LuigiTestException("Wrapper task output raises test exception!")
