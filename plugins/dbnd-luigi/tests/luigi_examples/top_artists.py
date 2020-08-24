# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
import random

from collections import defaultdict
from datetime import date, datetime
from heapq import nlargest

import luigi
import luigi.contrib.hdfs
import luigi.contrib.postgres
import luigi.contrib.spark

from dbnd import log_metric
from dbnd._vendor.namesgenerator import get_random_name


class LuigiTestException(Exception):
    pass


logger = logging.getLogger("user_log")


class Streams(luigi.Task):
    """
    Faked version right now, just generates bogus data.
    """

    date = luigi.DateParameter(default=date.today())

    version = luigi.Parameter(default=None)

    def run(self):
        """
        Generates bogus data and writes it into the :py:meth:`~.Streams.output` target.
        """
        logger.warning("Hey, this is streams task!")

        with self.output().open("w") as output:
            for _ in range(1000):
                output.write(
                    "{} {} {}\n".format(
                        random.randint(0, 999),
                        get_random_name(),
                        random.randint(0, 999),
                    )
                )
            log_metric("lines", 1000)

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in the local file system.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(
            os.path.abspath(
                "data/streams_%s__%s_faked.tsv"
                % (self.date.strftime(" %Y_%m_%d"), self.version)
            )
        )


class AggregateArtists(luigi.Task):
    """
    This task runs over the target data returned by :py:meth:`~/.Streams.output` and
    writes the result into its :py:meth:`~.AggregateArtists.output` target (local file).
    """

    date_interval = luigi.DateIntervalParameter(default=date.today())

    version = luigi.Parameter(default=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(
            os.path.abspath(
                "data/artist_streams_{}__{}.tsv".format(
                    self.date_interval, self.version
                )
            )
        )

    def requires(self):
        """
        This task's dependencies:
        * :py:class:`~.Streams`
        :return: list of object (:py:class:`luigi.task.Task`)
        """
        return [Streams(date, version=self.version) for date in self.date_interval]

    def run(self):
        logger.warning("Hey, this is aggregate artists task!")
        artist_count = defaultdict(int)

        for t in self.input():
            with t.open("r") as in_file:
                for line in in_file:
                    _, artist, track = line.strip().split()
                    artist_count[artist] += 1

        with self.output().open("w") as out_file:
            for artist, count in artist_count.items():
                out_file.write("{}\t{}\n".format(artist, count))


class Top10Artists(luigi.Task):
    """
    This task runs over the target data returned by :py:meth:`~/.AggregateArtists.output` or
    :py:meth:`~/.AggregateArtistsSpark.output` in case :py:attr:`~/.Top10Artists.use_spark` is set and
    writes the result into its :py:meth:`~.Top10Artists.output` target (a file in local filesystem).
    """

    date_interval = luigi.DateIntervalParameter(default=date.today())
    version = luigi.Parameter(default=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

    def requires(self):
        """
        This task's dependencies:
        * :py:class:`~.AggregateArtists` or
        * :py:class:`~.AggregateArtistsSpark` if :py:attr:`~/.Top10Artists.use_spark` is set.
        :return: object (:py:class:`luigi.task.Task`)
        """
        return AggregateArtists(self.date_interval, version=self.version)

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(
            os.path.abspath(
                "data/top_artists_%s_%s.tsv" % (self.date_interval, self.version)
            )
        )

    def run(self):
        from dbnd import log_metric

        logger.warning("Hey, this is top artists task!")
        top_10 = nlargest(10, self._input_iterator())
        log_metric(key="Top10Artists", value=str(top_10))
        with self.output().open("w") as out_file:
            for streams, artist in top_10:
                out_line = "\t".join(
                    [
                        str(self.date_interval.date_a),
                        str(self.date_interval.date_b),
                        artist,
                        str(streams),
                    ]
                )
                out_file.write((out_line + "\n"))

    def _input_iterator(self):
        with self.input().open("r") as in_file:
            for line in in_file:
                artist, streams = line.strip().split()
                yield int(streams), artist


class Top10ArtistsRunException(Top10Artists):
    def run(self):
        raise LuigiTestException("Run throws a test exception!")


class Top10ArtistsRequiresException(Top10Artists):
    def requires(self):
        raise LuigiTestException("Requires throws a test exception!")


class Top10ArtistsOutputException(Top10Artists):
    def output(self):
        raise LuigiTestException("Output throws a test exception!")


class MyPostgresQuery(luigi.contrib.postgres.PostgresQuery):
    host = "localhost"
    database = "databand"
    user = "databand"
    password = "databand"
    table = "dbnd_task_run_v2"
    query = "select * from dbnd_task_run_v2 limit 10000"

    def run(self):
        return True
