import logging

import dbnd

from dbnd import config
from dbnd_examples.data import dbnd_examples_data_path


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_setup_plugin():
    # register configs
    from dbnd_examples.pipelines.wine_quality import wine_quality_decorators
    from dbnd_examples import feature_data

    str([wine_quality_decorators, feature_data])
    config.set_from_config_file(dbnd_examples_data_path("examples_config.cfg"))
