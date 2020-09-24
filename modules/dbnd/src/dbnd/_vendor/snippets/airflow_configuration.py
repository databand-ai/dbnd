# -*- coding: utf-8 -*-
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

import os

import six


def generate_fernet_key():
    try:
        from cryptography.fernet import Fernet
    except ImportError:
        return ''
    else:
        return Fernet.generate_key().decode()


def expand_env_var(env_var):
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    if not isinstance(env_var, six.string_types):
        # we don't support interpolation of non string values for now
        # sometime configparser can have complex structures, without this condition
        # this will make that objects strings
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(env_var))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated
