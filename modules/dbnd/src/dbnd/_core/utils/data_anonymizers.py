# © Copyright Databand.ai, an IBM Company 2022

import re


# THIS FILE IS DUPLICATION OF modules/dbnd-web/src/dbnd_web/internal_frameworks/data_anonymization/data_anonymizers.py


SECRET_NAMES = [
    "secret",
    "password",
    "private[_-]key",
    "access[_-]key",
    "aws[_-]key",
    "token",
    "authorization",
    "passphrase",
    "aws:iam:",
]
KEYS = r"(" + "|".join(SECRET_NAMES) + ")"

DEFAULT_MASKING_VALUE = "***"
MASKING_VALUE = rf"\g<keep>{DEFAULT_MASKING_VALUE}"

IDENTIFIER = (
    r"[\w-]*"  # Multiple characters with a - separation between them like aaa-bbb-ccc
)
SECRET_VALUE = r"[\w\.+-/]+"  # See here: https://stackoverflow.com/questions/6102077/possible-characters-base64-url-safe-function
SEPARATOR = r"\s*[=: ]\s*"  # Spaces (since this can be json serialized with either = or : before the value
QUOTES = r"['\"]?"  # Single or double quote (since this is json serialized)

COMBINED_REGEX = (
    rf"(?P<keep>{KEYS}{IDENTIFIER}{QUOTES}{SEPARATOR}{QUOTES}){SECRET_VALUE}"
)
masking_pattern = re.compile(COMBINED_REGEX, flags=re.IGNORECASE)


def mask_sensitive_data(data: str) -> str:
    """
    This function is used to anonymize values inside strings - matching the values by a regex pattern.
    """
    if not data:
        return data

    # Will raise TypeError if data is not str
    result = masking_pattern.sub(MASKING_VALUE, data)

    return result
