# © Copyright Databand.ai, an IBM Company 2022

import pendulum
import pkg_resources

from dbnd._vendor import version


pendulum_version = pkg_resources.get_distribution("pendulum").version
if version.parse(pendulum_version) >= version.parse("2.0.0"):
    pendulum_max_dt = pendulum.DateTime.max
    pendulum_min_dt = pendulum.DateTime.min
else:
    pendulum_max_dt = pendulum.max
    pendulum_min_dt = pendulum.min
