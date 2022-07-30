# © Copyright Databand.ai, an IBM Company 2022

import time

import luigi


class InputText(luigi.ExternalTask):
    """
    This class represents something that was created elsewhere by an external process,
    so all we want to do is to implement the output method.
    """

    name = luigi.Parameter()

    def output(self):
        """
        Returns the target output for this task.
        In this case, it expects a file to be present in the local file system.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget("/var/tmp/text/%s.txt" % self.name)


class Bar(luigi.Task):
    num = luigi.IntParameter()

    def run(self):
        time.sleep(1)
        self.output().open("w").close()

    def output(self):
        """
        Returns the target output for this task.
        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        time.sleep(1)
        return luigi.LocalTarget("/var/tmp/bar/%d" % self.num)


class Foo(luigi.Task):
    num = luigi.IntParameter()

    def requires(self):
        """
        This task's dependencies:
        * :py:class:`~.InputText`
        :return: list of object (:py:class:`luigi.task.Task`)
        """
        yield InputText(name="no_way")

    def run(self):
        time.sleep(1)
        self.output().open("w").close()

    def output(self):
        """
        Returns the target output for this task.
        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        time.sleep(1)
        return luigi.LocalTarget("/var/tmp/bar/%d" % self.num)


class WordCount(luigi.Task):
    def requires(self):
        """
        This task's dependencies:
        * :py:class:`~.InputText`
        :return: list of object (:py:class:`luigi.task.Task`)
        """
        yield InputText(name="jonny")
        yield InputText(name="no")
        yield Foo(num=2)
        yield Bar(num=1)

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget("/var/tmp/text-count/%s" % 1)

    def run(self):
        """
        1. count the words for each of the :py:meth:`~.InputText.output` targets created by :py:class:`~.InputText`
        2. write the count into the :py:meth:`~.WordCount.output` target
        """
        count = {}

        # NOTE: self.input() actually returns an element for the InputText.output() target
        for (
            f
        ) in (
            self.input()
        ):  # The input() method is a wrapper around requires() that returns Target objects
            for line in f.open(
                "r"
            ):  # Target objects are a file system/format abstraction and this will return a file stream object
                for word in line.strip().split():
                    count[word] = count.get(word, 0) + 1

        # output data
        f = self.output().open("w")
        for word, count in count.items():
            f.write("%s\t%d\n" % (word, count))
        f.close()  # WARNING: file system operations are atomic therefore if you don't close the file you lose all data
