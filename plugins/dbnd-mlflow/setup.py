from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-mlflow",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "six"],
    extras_require=dict(tests=[]),
    entry_points={
        "dbnd": ["dbnd-mlflow = dbnd_mlflow._plugin"],
        "mlflow.tracking_store": [
            "dbnd = dbnd_mlflow.tracking_store:get_dbnd_store",
            "dbnd+s = dbnd_mlflow.tracking_store:get_dbnd_store",
            "databand = dbnd_mlflow.tracking_store:get_dbnd_store",
            "databand+s = dbnd_mlflow.tracking_store:get_dbnd_store",
        ],
    },
)
