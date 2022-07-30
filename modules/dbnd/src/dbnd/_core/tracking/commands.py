# © Copyright Databand.ai, an IBM Company 2022

import logging

import six

from dbnd._core.current import try_get_current_task_run


logger = logging.getLogger(__name__)


def set_external_resource_urls(links):
    # type: (dict[str, str]) -> None
    """
    Will add the `links` as external resources, to the current running task.
    If there is no running task or the links are bad formatted it will fail without raising.

    @param links: map between the name of the resource and the link to the resource
    """
    if not isinstance(links, dict):
        logger.warning("Failed to add links as external resources: links is not a dict")
        return

    links = {item: value for item, value in six.iteritems(links) if value}
    if not links:
        logger.warning(
            "Failed to add links as external resources: links is empty dict or dict with None values "
        )
        return

    task_run = try_get_current_task_run()
    if task_run is None:
        logger.warning(
            "Failed to add links as external resources: There is no running task"
        )
        return

    try:
        task_run.set_external_resource_urls(links)
    except Exception as e:
        logger.warning(
            "Failed to add links as external resources: raised {e}".format(e=e)
        )
