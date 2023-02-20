# -*- coding: utf-8 -*-
# Vendorized from Apache Airflow
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
#
# This file has been modified by databand.ai to support dbnd orchestration.

import threading

from airflow.exceptions import AirflowTaskTimeout
from airflow.utils.log.logging_mixin import LoggingMixin


class timeout(LoggingMixin):
    """
    To be used in a ``with`` block and timeout its content.
    """

    def __init__(self, seconds=1, error_message="Timeout"):
        self.seconds = seconds
        self.error_message = error_message
        self.timer = threading.Timer(seconds, self.handle_timeout)

    def handle_timeout(self, signum, frame):
        self.log.error("Process timed out")
        raise AirflowTaskTimeout(self.error_message)

    def __enter__(self):
        try:
            self.timer.start()
        except ValueError as e:
            self.log.warning("timeout can't be used in the current context")
            self.log.exception(e)

    def __exit__(self, type, value, traceback):
        try:
            self.timer.cancel()
        except ValueError as e:
            self.log.warning("timeout can't be used in the current context")
            self.log.exception(e)
