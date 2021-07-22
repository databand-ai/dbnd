import re

from typing import Callable, Optional

import more_itertools


def camel_to_snake(name, placeholder="_"):
    # type: (str, str) -> str
    # Convert argument names from lowerCamelCase to snake case.
    # "AbBcDe" -> "ab_bc_de"
    return re.sub(
        r"[A-Z]",
        lambda x: (
            placeholder if x.start(0) > 0 else ""
        )  # don't add to the first character
        + x.group(0).lower(),
        name,
    )


def clean_job_name(
    value, enabled_characters=r"\-_", placeholder="_", max_size=None, postfix=None
):
    # type:(str,str,str, int, Optional[str]) -> str
    """
    @param value: the base value to transform
    @param enabled_characters: all the allowed characters beside alphanumeric [white list]
    @param placeholder: the char that would be used to replace any character that is not alphanumeric or in `enabled_characters`
    @param max_size: the maximum length allowed for the output
    @param postfix: optional string to add to end of the result value
    @return: a transformation of the value, by all the configurations
    """
    value = camel_to_snake(value, placeholder=placeholder)
    enabled_characters = re.escape(enabled_characters)
    # clean all garbage
    value = re.sub(r"[^a-z0-9%s]" % enabled_characters, placeholder, value)

    # clean all duplicated special charaters:  .-  or -- or ___
    value = re.sub(
        r"([{enabled_characters}])[{enabled_characters}]+".format(
            enabled_characters=enabled_characters
        ),
        r"\1",
        value,
    )
    if max_size:
        if postfix:
            max_size -= len(postfix)
        value = value[:max_size]

    if postfix:
        postfix = clean_job_name(
            value=postfix,
            enabled_characters=enabled_characters,
            placeholder=placeholder,
        )
        value += postfix
        # different from the first replace, we are replacing using second character
        value = re.sub(
            r"[{enabled_characters}]+([{enabled_characters}])".format(
                enabled_characters=enabled_characters
            ),
            r"\1",
            value,
        )
    return value


def str_or_none(value):
    if value is None:
        return None
    return str(value)


def safe_short_string(value, max_value_len=1000, tail=False):
    """Returns the string limited by max_value_len parameter.

    Parameters:
        value (str): the string to be shortened
        max_value_len (int): max len of output
        tail (bool):
    Returns:
        str: the string limited by max_value_len parameter

    >>> safe_short_string('abcdefghijklmnopqrstuvwxyz', max_value_len=20)
    'abcdef... (6 of 26)'
    >>> safe_short_string('abcdefghijklmnopqrstuvwxyz', max_value_len=20, tail=True)
    '(6 of 26) ...uvwxyz'
    >>> (safe_short_string(''), safe_short_string(None))
    ('', None)
    >>> safe_short_string('qwerty', max_value_len=4)
    '... (0 of 6)'
    >>> safe_short_string('qwerty', max_value_len=-4)
    '... (0 of 6)'
    >>> safe_short_string('qwerty'*123, max_value_len=0)
    '... (0 of 738)'
    >>> safe_short_string('qwerty', max_value_len=4, tail=True)
    '(0 of 6) ...'
    >>> safe_short_string('qwerty', max_value_len=-4, tail=True)
    '(0 of 6) ...'
    >>> safe_short_string('qwerty'*123, max_value_len=0, tail=True)
    '(0 of 738) ...'
    >>> safe_short_string('qwerty', max_value_len='exception')
    "ERROR: Failed to shorten string: '>' not supported between instances of 'int' and 'str'"
    """
    try:
        if not value:
            return value
        if len(value) > max_value_len:
            placeholder = "... (%s of %s)".format(max_value_len, len(value))
            actual_len = max_value_len - len(placeholder)
            actual_len = 0 if actual_len < 0 else actual_len
            if tail:
                value = "(%s of %s) ...%s" % (
                    actual_len,
                    len(value),
                    value[len(value) - actual_len :],
                )
            else:
                value = "%s... (%s of %s)" % (
                    value[:actual_len],
                    actual_len,
                    len(value),
                )
        return value
    except Exception as ex:
        # we don't want to fail here
        return "ERROR: Failed to shorten string: %s" % ex


def pluralize(s, n, plural_form=None):
    if n == 1:
        return s
    else:
        return plural_form or s + "s"


def strip_whitespace(string):
    return re.sub(r"\s+", " ", string.strip(), flags=re.UNICODE)


def is_task_name_driver(task_name):
    return "dbnd_driver" in task_name


# Regex to catch start of dbnd logs (they're starting like this: [2020-05-07 17:47:07,768])
DBND_LOGS_REGEX = re.compile(r"\[\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3}]")


def merge_dbnd_and_spark_logs(dbnd, spark):
    """Merges dbnd and spark logs into single array and sorts them by timestamp provided.
    dbnd logs are multi-lined so we have to perform a tricky comparison and append here.
    """
    result = []

    dbnd_idx = 0
    spark_idx = 0

    while True:
        dbnd_line = None if dbnd_idx >= len(dbnd) else dbnd[dbnd_idx]
        spark_line = None if spark_idx >= len(spark) else spark[spark_idx]

        if spark_line is None and dbnd_line is None:
            break

        if spark_line is None:
            result.append(dbnd_line)
            dbnd_idx += 1
            continue

        if dbnd_line is None:
            result.append(spark_line)
            spark_idx += 1
            continue

        # dbnd logs are multi-lined and the first line is timestamp starting from [.
        # However, the rest of lines in the log record can also start with '[' so we have to check the line with regex
        if dbnd_line.startswith("[") and DBND_LOGS_REGEX.match(dbnd_line):
            if dbnd_line < spark_line:
                result.append(dbnd_line)
                dbnd_idx += 1
            else:
                result.append(spark_line)
                spark_idx += 1
        else:
            result.append(dbnd_line)
            dbnd_idx += 1

    return result


def strip_by(predicate, string):
    # type: (Callable[[str], bool], str) -> str
    """
    striping any characters in the beginning or the end matching the predicate
    >>> strip_by(lambda c: not c.isalnum(), "...123123.123213.123..asd.22..") == "123123.123213.123..asd.22"
    """
    return "".join(more_itertools.strip(string, predicate))
