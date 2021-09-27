"""
https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
A client-provided string that refers to an object in a resource URL, such as /api/v1/pods/some-name.

DNS-1123 Rules:
* contain only lowercase alphanumeric characters, '-' or '.'
* start with an alphanumeric character
* end with an alphanumeric character
* if subdomain name - contain no more than 253 characters
* if label name - contain at most 63 characters.
"""
import functools
import typing

from dbnd._core.utils.string_utils import clean_job_name, strip_by


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun
    from typing import Optional

MAX_CLEAN_SUBDOMAIN_NAME_DNS1123_LEN = 253
MAX_CLEAN_LABEL_NAME_DNS1123_LEN = 63


def clean_name_dns1123(
    value, max_size, postfix=None,
):
    # type:(str, int, Optional[str]) -> str
    """
    Create a dns-1123 compatible name.

    @param value: the base value to transform
    @param max_size: the maximum length allowed for the output
    @param postfix: optional string to add to end of the result value
    @return: dns-1123 compatible name
    """
    cleaned_value = clean_job_name(
        value=value,
        enabled_characters="-.",
        placeholder="-",
        max_size=max_size,
        postfix=postfix,
    )

    # remove any none alphanumeric characters in the beginning and end of the cleaned value
    return strip_by(lambda c: not c.isalnum(), cleaned_value)


# Create a dns-1123 compatible label name
clean_label_name_dns1123 = functools.partial(
    clean_name_dns1123, max_size=MAX_CLEAN_LABEL_NAME_DNS1123_LEN
)

# Create a dns-1123 compatible subdomain name
clean_subdomain_name_dns1123 = functools.partial(
    clean_name_dns1123, max_size=MAX_CLEAN_SUBDOMAIN_NAME_DNS1123_LEN
)


def create_pod_id(task_run):
    # type:(TaskRun) -> str
    """
    DNS-1123 subdomain name from task_run
    """

    return clean_subdomain_name_dns1123(
        "dbnd.{task_family}.{task_name}".format(
            task_family=task_run.task.task_family, task_name=task_run.task.task_name,
        ),
        postfix=".%s" % str(task_run.task_run_uid)[:8],
    )
