# -*- coding: utf-8 -*-
# © Copyright Databand.ai, an IBM Company 2022

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# This file has been modified by databand.ai to support dbnd orchestration.

import logging
import os
import sys

from copy import deepcopy

from airflow.config_templates.airflow_local_settings import (
    DEFAULT_LOGGING_CONFIG as airflow_default_log_config,
)
from airflow.configuration import conf
from airflow.utils.log.file_processor_handler import FileProcessorHandler


windows_compatible_mode = os.name == "nt"


class FileProcessorHandlerWinCompatible(FileProcessorHandler):
    """
    FileProcessorHandler is a python log handler that handles
    dag processor logs. It creates and delegates log handling
    to `logging.FileHandler` after receiving dag processor context.
    """

    def set_context(self, filename):
        """
        Provide filename context to airflow task handler.
        :param filename: filename in which the dag is located
        """
        super(FileProcessorHandlerWinCompatible, self).set_context(filename)

        logging.debug(
            "Processor logging is up at %s for %s", self.handler.baseFilename, filename
        )

    def _symlink_latest_log_directory(self):
        """
        Create symbolic link to the current day's log directory to
        allow easy access to the latest scheduler log files.

        :return: None
        """

        if windows_compatible_mode:
            return
        super(FileProcessorHandlerWinCompatible, self)._symlink_latest_log_directory()


class FileProcessorErrorHandler(logging.StreamHandler):
    def __init__(self, **kwargs):
        super(FileProcessorErrorHandler, self).__init__(**kwargs)

    def emit(self, record):
        if "ERROR" in record.msg:
            super(FileProcessorErrorHandler, self).emit(record)


PROCESSOR_LOG_FOLDER = conf.get("scheduler", "CHILD_PROCESS_LOG_DIRECTORY")

# this config will be used automatically by airflow,
# but we will override it the first time we can (right after import)
DEFAULT_LOGGING_CONFIG = deepcopy(airflow_default_log_config)
# we need to replace the original with this one for windows support
wp = "%s.%s" % (__name__, FileProcessorHandlerWinCompatible.__name__)
error_handler = "%s.%s" % (__name__, FileProcessorErrorHandler.__name__)

DEFAULT_LOGGING_CONFIG["handlers"]["processor"]["class"] = wp

# the default airflow "console" handler somehow ends up redirecting back to processor logger, getting stuck in an infinite loop
# sys.stdout at this point is Raw stdout without redirection, as this code runs at airflow boot time
# ( no airflow task actually is running, so there is no stdout_redirect applied )
DEFAULT_LOGGING_CONFIG["handlers"]["stdout"] = {
    "class": error_handler,
    "formatter": "airflow",
    "stream": sys.stdout,
    "level": "INFO",
}

DEFAULT_LOGGING_CONFIG["loggers"]["airflow.processor"]["handlers"].append("stdout")
