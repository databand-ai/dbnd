import datetime
import random

from collections import defaultdict
from datetime import timedelta
from heapq import nlargest

import six

from dbnd import data, output, parameter
from dbnd.tasks import DataSourceTask, PipelineTask, PythonTask
from targets import target


class ExternalStream(DataSourceTask):
    root_location = data.config("dbnd_examples", "root_streams")
    task_target_date = parameter[datetime.date]

    stream = output

    def band(self):
        self.stream = target(
            self.root_location, "%s.txt" % self.task_target_date.strftime("%Y-%m-%d")
        )


class Stream(PythonTask):
    task_target_date = parameter[datetime.date]
    stream = output.data

    def run(self):
        lines = [
            "{} {} {}".format(
                random.randint(0, 999), random.randint(0, 999), random.randint(0, 999)
            )
            for _ in range(1000)
        ]
        self.stream.write("\n".join(lines))


class ArtistAggregator(PythonTask):
    streams = parameter.data
    index = output.data

    def run(self):
        artist_count = defaultdict(int)

        for s in self.streams:
            for l in s.readlines():
                _, artist, track = l.strip().split()
                artist_count[artist] += 1
        for artist, count in six.iteritems(artist_count):
            self.index.write("{}\t{}\n".format(artist, count))


class TopNArtists(PythonTask):
    artists = parameter.data
    n_largest = parameter.value(10)

    output = output.data

    def run(self):
        top_n = nlargest(self.n_largest, self._input_iterator())
        values = ["{}\t{}".format(streams, artist) for streams, artist in top_n]
        self.output.write("\n".join(values))

    def _input_iterator(self):
        for line in self.artists.readlines():
            artist, streams = line.strip().split()
            yield int(streams), int(artist)


class AggregateTopArtists(PipelineTask):
    period = parameter.value(timedelta(days=2))

    def band(self):
        streams = [
            Stream(task_name="Stream_%s" % i, task_target_date=d).stream
            for i, d in enumerate(period_dates(self.task_target_date, self.period))
        ]
        artists = ArtistAggregator(streams=streams)
        top_n = TopNArtists(artists=artists.index)
        return top_n
