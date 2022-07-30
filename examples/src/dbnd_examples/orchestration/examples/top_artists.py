# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging
import random

from collections import defaultdict
from datetime import timedelta
from heapq import nlargest
from typing import List

import six

from dbnd import band, data, parameter, task
from dbnd.utils import data_combine, period_dates
from targets import target
from targets.types import DataList


@band(
    root_location=data.config("dbnd_examples", "root_streams"),
    task_target_date=parameter[datetime.date],
)
def external_stream(root_location, task_target_date):
    return target(root_location, "%s.txt" % task_target_date.strftime("%Y-%m-%d"))


@task
def stream(seed=1):
    r = random.Random(x=seed)
    return [
        "{} {} {}".format(r.randint(0, 999), r.randint(0, 999), r.randint(0, 999))
        for _ in range(1000)
    ]


@task
def aggregate_artists(stream):
    # type: (DataList[str]) -> List[str]
    artist_count = defaultdict(int)
    for l in stream:
        _, artist, track = l.strip().split()
        artist_count[artist] += 1

    return [
        "{}\t{}".format(artist, count) for artist, count in six.iteritems(artist_count)
    ]


@task
def top_n_artists(artists, n_largest=10):
    # type: (DataList[str], int) -> List[str]
    def _input_iterator():
        for line in artists:
            artist, streams = line.strip().split()
            yield int(streams), int(artist)

    top_n = nlargest(n_largest, _input_iterator())
    return ["{}\t{}".format(streams, artist) for streams, artist in top_n]


@band
def top_artists_report(task_target_date, period=timedelta(days=2)):
    logging.info("top_artists_report")
    streams = [
        stream(task_name="Stream_%s" % i, task_target_date=d)
        for i, d in enumerate(period_dates(task_target_date, period))
    ]
    artists = aggregate_artists(stream=data_combine(streams))
    top_n = top_n_artists(artists=artists)
    return top_n


@band
def top_artists_big_report():
    return top_artists_report(period="3d", override={top_n_artists.task.n_largest: 15})
