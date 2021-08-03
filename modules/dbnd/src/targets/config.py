import abc
import logging
import os.path
import random
import tempfile

from dbnd._core.current import try_get_databand_context, try_get_databand_run


logger = logging.getLogger(__name__)

_CONFIG_PARSER = None


class TargetConfigProvider(object):
    @abc.abstractmethod
    def get_config_section_values(self):
        pass

    @abc.abstractmethod
    def get_config_value(self, section, value):
        pass


def set_config_provider(config_parser):
    global _CONFIG_PARSER
    _CONFIG_PARSER = config_parser


def get_config_section_values(section):
    config_provider = _CONFIG_PARSER
    if not config_provider:
        return {}
    return config_provider.get_config_section_values(section)


def get_local_tempfile(*path):
    run = try_get_databand_run()
    if run:
        dbnd_local_root = run.get_current_dbnd_local_root()
        if dbnd_local_root.exists():
            # on remote engine temp defined at driver can be un-awailable
            # simple workaround to use tmp folder on the machine
            tempdir = dbnd_local_root.partition("tmp").path
        else:
            # fallback to simple temp dir
            tempdir = tempfile.gettempdir()
    else:
        tempdir = tempfile.gettempdir()

    path = os.path.join(tempdir, "databand-tmp-%09d" % random.randrange(0, 1e10), *path)
    base_dir = os.path.dirname(path)
    try:
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
    except Exception as ex:
        logger.info("Failed to create temp dir %s: %s", base_dir, ex)
    return path


def is_in_memory_cache_target_value():
    dc = try_get_databand_context()
    if dc:
        return dc.settings.run.target_cache_on_access
    return False
