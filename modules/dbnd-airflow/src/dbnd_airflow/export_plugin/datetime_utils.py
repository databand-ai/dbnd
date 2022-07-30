# © Copyright Databand.ai, an IBM Company 2022

from distutils.version import LooseVersion

import pendulum
import pkg_resources


pendulum_version = pkg_resources.get_distribution("pendulum").version
if LooseVersion(pendulum_version) >= LooseVersion("2.0.0"):
    pendulum_max_dt = pendulum.DateTime.max
    pendulum_min_dt = pendulum.DateTime.min
else:
    pendulum_max_dt = pendulum.max
    pendulum_min_dt = pendulum.min
