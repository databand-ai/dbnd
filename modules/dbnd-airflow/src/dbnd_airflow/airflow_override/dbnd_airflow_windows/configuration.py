from __future__ import absolute_import, unicode_literals

import json
import os

from tempfile import mkstemp

from airflow import configuration as conf


# os.fchmod returns error on windows
def tmp_configuration_copy(chmod=0o600):
    """
    Returns a path for a temporary file including a full copy of the configuration
    settings.
    :return: a path to a temporary file
    """
    cfg_dict = conf.as_dict(display_sensitive=True, raw=True)
    temp_fd, cfg_path = mkstemp()

    with os.fdopen(temp_fd, "w") as temp_file:
        json.dump(cfg_dict, temp_file)

    return cfg_path
