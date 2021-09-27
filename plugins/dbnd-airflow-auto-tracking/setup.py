import os

import setuptools

from setuptools.config import read_configuration


BASE_PATH = os.path.dirname(__file__)
CFG_PATH = os.path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]


setuptools.setup(
    name="dbnd-airflow-auto-tracking",
    package_dir={"": "src"},
    # we are not requiring airflow, as this plugin should be installed into existing airflow deployment
    install_requires=[
        "dbnd==" + version,
        "dbnd-airflow==" + version,
        "dbnd-airflow-monitor==" + version,
    ],
    entry_points={
        "airflow.plugins": [
            "dbnd_airflow_auto_tracking = dbnd_airflow_auto_tracking.dbnd_airflow_auto_tracking:DbndAutoTracking"
        ]
    },
)
