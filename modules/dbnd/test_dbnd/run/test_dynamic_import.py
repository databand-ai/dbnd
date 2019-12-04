# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
# Modifications copyright (C) 2018 databand.ai
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

import dbnd

from dbnd._core.inline import run_cmd_locally


class TestDynamicImport(object):
    def test_dynamic_loading(self):
        run_cmd_locally(
            [
                "--module",
                "test_dbnd.scenarios.do_not_import",
                "DynamicImportTask",
                "-r",
                "x=123",
            ]
        )
