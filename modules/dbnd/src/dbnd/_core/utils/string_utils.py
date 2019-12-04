import re


MAX_CLEAN_NAME_DNS1123_LEN = 253


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


def clean_job_name_dns1123(
    value,
    enabled_characters="-.",
    placeholder="-",
    postfix=None,
    max_size=MAX_CLEAN_NAME_DNS1123_LEN,
):
    return clean_job_name(
        value=value,
        enabled_characters=enabled_characters,
        placeholder=placeholder,
        max_size=max_size,
        postfix=postfix,
    )


def str_or_none(value):
    if value is None:
        return None
    return str(value)


def safe_short_string(value, max_value_len=1000):
    try:
        if value is None:
            return value

        if value and len(value) > max_value_len:
            suffix_tmp = "... (%s of %s)".format(max_value_len, len(value))
            actual_len = max_value_len - len(suffix_tmp) - 2
            value = "%s... (%s of %s)" % (value[:actual_len], actual_len, len(value))
        return value
    except Exception as ex:
        # we don't want to fail here
        return "ERROR: Failed to shorten string: %s" % ex


def pluralize(s, n, plural_form=None):
    if n == 1:
        return s
    else:
        return plural_form or s + "s"
