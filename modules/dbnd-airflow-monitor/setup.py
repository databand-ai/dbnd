from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-airflow-monitor",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "setuptools", "prometheus_client"],
    extras_require={
        "tests": ["pytest", "mock", "sh"],
        "composer": [
            "PyJWT==1.7.1",
            "cryptography==37.0.2",
            "google-auth==1.10.0",
            "requests==2.22.0",
            "requests_toolbelt==0.9.1",
            "tzlocal>=1.5.1",
        ],
    },
    entry_points={"dbnd": ["airflow-monitor = airflow_monitor._plugin"]},
)
