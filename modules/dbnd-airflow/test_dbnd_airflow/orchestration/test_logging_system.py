# © Copyright Databand.ai, an IBM Company 2022

from __future__ import absolute_import

from airflow.logging_config import configure_logging


class TestDatabandLogging(object):
    def test_logging(self):
        configure_logging()
