import abc
import operator

from dbnd._core.errors import friendly_error


class Validator(object):
    @property
    @abc.abstractmethod
    def description(self):
        pass

    @abc.abstractmethod
    def validate(self, parameter, value):
        pass


class NonEmptyString(Validator):
    @property
    def description(self):
        return "Non empty string"

    def validate(self, parameter, value):
        if not value:
            raise friendly_error.config.empty_string_validator(parameter)
        return value


class ChoiceValidator(Validator):
    """
    A parameter which takes two values:
        1. an instance of :class:`~collections.Iterable` and
        2. the class of the variables to convert to.

    In the task definition, use

    .. code-block:: python

        class MyTask(dbnd.Task):
            my_param = parameter.choices([0.1, 0.2, 0.3])[float]

    At the command line, use

    .. code-block:: console

        $ dbnd --module my_tasks MyTask --my-param 0.1

    Consider using :class:`~databand.EnumParameter` for a typed, structured
    alternative.  This class can perform the same role when all choices are the
    same type and transparency of parameter value on the command line is
    desired.
    """

    def __init__(self, choices):
        """
        :param function value_type: The type of the input variable, e.g. str, int,
                                  float, etc.
                                  Default: str
        :param choices: An iterable, all of whose elements are of `value_type` to
                        restrict parameter choices to.
        """

        super(ChoiceValidator, self).__init__()
        self._choices = set(choices)

    @property
    def description(self):
        return "Choices: {" + ", ".join(str(choice) for choice in self._choices) + "}"

    def validate(self, parameter, value):

        assert all(
            parameter.value_type.is_type_of(choice) for choice in self._choices
        ), "Invalid type in choices"

        if value not in self._choices:
            raise friendly_error.config.choice_error(
                parameter, value=value, choices=self._choices
            )
        return value


class NumericalValidator(Validator):
    """
    Parameter whose value is a number of the specified type, e.g. ``int`` or
    ``float`` and in the range specified.

    In the task definition, use

    .. code-block:: python

        class MyTask(dbnd.Task):
            my_param_1 = databand.NumericalParameter(
                value_type=int, min_value=-3, max_value=7) # -3 <= my_param_1 < 7
            my_param_2 = databand.NumericalParameter(
                value_type=int, min_value=-3, max_value=7, left_op=operator.lt, right_op=operator.le) # -3 < my_param_2 <= 7

    At the command line, use

    .. code-block:: console

        $ dbnd --module my_tasks MyTask --my-param-1 -3 --my-param-2 -2
    """

    def __init__(self, min_value, max_value, left_op=operator.le, right_op=operator.lt):
        """
        :param function value_type: The type of the input variable, e.g. int or float.
        :param min_value: The minimum value permissible in the accepted values
                          range.  May be inclusive or exclusive based on left_op parameter.
                          This should be the same type as value_type.
        :param max_value: The maximum value permissible in the accepted values
                          range.  May be inclusive or exclusive based on right_op parameter.
                          This should be the same type as value_type.
        :param function left_op: The comparison operator for the left-most comparison in
                                 the expression ``min_value left_op value right_op value``.
                                 This operator should generally be either
                                 ``operator.lt`` or ``operator.le``.
                                 Default: ``operator.le``.
        :param function right_op: The comparison operator for the right-most comparison in
                                  the expression ``min_value left_op value right_op value``.
                                  This operator should generally be either
                                  ``operator.lt`` or ``operator.le``.
                                  Default: ``operator.lt``.
        """
        self._min_value = min_value
        self._max_value = max_value

        self._left_op = left_op
        self._right_op = right_op
        self._permitted_range = (
            " {left_endpoint}{min_value}, {max_value}{right_endpoint}".format(
                min_value=self._min_value,
                max_value=self._max_value,
                left_endpoint="[" if left_op == operator.le else "(",
                right_endpoint=")" if right_op == operator.lt else "]",
            )
        )

    @property
    def description(self):
        return "permitted values: " + self._permitted_range

    def validate(self, parameter, value):
        if self._left_op(self._min_value, value) and self._right_op(
            value, self._max_value
        ):
            return value
        else:
            raise ValueError(
                "{s} is not in the set of {permitted_range}".format(
                    s=value, permitted_range=self._permitted_range
                )
            )
