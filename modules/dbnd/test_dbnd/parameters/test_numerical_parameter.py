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
#
# This file has been modified by databand.ai to support dbnd orchestration.

from dbnd import parameter


class TestNumericalParameter(object):
    def test_int_min_value_inclusive(self):
        d = parameter.numerical(min_value=-3, max_value=7)[int]._p
        assert -3 == d.parse_from_str(-3)

    def test_float_min_value_inclusive(self):
        d = parameter.numerical(min_value=-3, max_value=-1)[float]._p
        d.validate(-3)

    # def test_int_min_value_exclusive(self):
    #     d = IntParameter.numerical(min_value=-3, max_value=7  )
    #     with pytest.raises(ValueError):
    #         d.validate(-3)
    #
    # def test_float_min_value_exclusive(self):
    #     d = IntParameter.numerical(min_value=-3, max_value=7)
    #     with pytest.raises(ValueError):
    #         d.validate(-3)

    # def test_int_max_value_inclusive(self):
    #     d = IntParameter.numerical(min_value=-3, max_value=7, left_op=le, right_op=le
    #                                )
    #     assert 7 == d.parse_from_str(7)
    #
    # def test_float_max_value_inclusive(self):
    #     d = parameters.NumericalParameter(
    #         value_type=float, min_value=-3, max_value=7, left_op=le, right_op=le
    #     )
    #     assert 7 == d.parse_from_str(7)
    #
    # def test_int_max_value_exclusive(self):
    #     d = IntParameter.numerical(min_value=-3, max_value=7, left_op=le, right_op=lt
    #                                )
    #     with pytest.raises(ValueError):
    #         d.validate(7)
    #
    # def test_float_max_value_exclusive(self):
    #     d = parameters.NumericalParameter(
    #         value_type=float, min_value=-3, max_value=7, left_op=le, right_op=lt
    #     )
    #     with pytest.raises(ValueError):
    #         d.validate(7)
    #
    # def test_defaults_start_range(self):
    #     d = IntParameter.numerical(min_value=-3, max_value=7)
    #     assert -3 == d.parse_from_str("-3")
    #
    # def test_endpoint_default_exclusive(self):
    #     d = IntParameter.numerical(min_value=-3, max_value=7)
    #     with pytest.raises(ValueError):
    #         d.validate(7)
    #
    # def test_value_type_parameter_exception(self):
    #     with pytest.raises(ParameterError):
    #         IntParameter.numerical(min_value=-3)
    #
    #
    # def test_int_serialize_parse(self):
    #     a = IntParameter.numerical(min_value=-3, max_value=7)
    #     b = -3
    #     assert b == a.parse_from_str(a.serialize(b))
