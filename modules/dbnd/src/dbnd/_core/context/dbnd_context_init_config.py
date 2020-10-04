import typing

from dbnd._core.utils.basics.helpers import parse_bool


if typing.TYPE_CHECKING:
    from dbnd._core.configuration.dbnd_config import DbndConfig


class _DbndContextInitConfig(object):
    """
    intersects with CoreConfig, but this one can be loaded without being dependent on any other Task objects
    """

    def __init__(self, conf_provider):
        self._conf = c = conf_provider  # type: DbndConfig
