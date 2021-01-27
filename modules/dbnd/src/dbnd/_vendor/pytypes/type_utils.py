# Copyright 2017 Stefan Richthofer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Created on 13.12.2016
import typing

from typing import Union


def get_Union_params(un):
    """Python version independent function to obtain the parameters
    of a typing.Union object.
    Tested with CPython 2.7, 3.5, 3.6 and Jython 2.7.1.
    """
    try:
        return un.__union_params__
    except AttributeError:
        # Python 3.6
        return un.__args__


def is_Union(tp):
    """Python version independent check if a type is typing.Union.
    Tested with CPython 2.7, 3.5, 3.6 and Jython 2.7.1.
    """
    if tp is Union:
        return True
    try:
        # Python 3.6
        return tp.__origin__ is Union
    except AttributeError:
        try:
            return isinstance(tp, typing.UnionMeta)
        except AttributeError:
            return False


def is_Tuple(tp):
    try:
        return isinstance(tp, typing.TupleMeta)
    except AttributeError:
        try:
            return isinstance(tp, typing._GenericAlias) and tp.__origin__ is tuple
        except AttributeError:
            return False


def get_Tuple_params(tpl):
    """Python version independent function to obtain the parameters
    of a typing.Tuple object.
    Omits the ellipsis argument if present. Use is_Tuple_ellipsis for that.
    Tested with CPython 2.7, 3.5, 3.6 and Jython 2.7.1.
    """
    try:
        return tpl.__tuple_params__
    except AttributeError:
        try:
            if tpl.__args__ is None:
                return None
            # Python 3.6
            if tpl.__args__[0] == ():
                return ()
            else:
                if tpl.__args__[-1] is Ellipsis:
                    return tpl.__args__[:-1] if len(tpl.__args__) > 1 else None
                else:
                    return tpl.__args__
        except AttributeError:
            return None
