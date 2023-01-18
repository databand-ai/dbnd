# © Copyright Databand.ai, an IBM Company 2022

import os

import setuptools


BASE_PATH = os.path.dirname(__file__)
CFG_PATH = os.path.join(BASE_PATH, "setup.cfg")

config = setuptools.config.read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-redshift",
    package_dir={"": "src"},
    install_requires=["psycopg2-binary", "dbnd==" + version, "redshift_connector"],
    extras_require={
        "tests": [
            'pandas==1.3.5;python_version<"3.8"',  # M1 is supported
            'pandas==1.4.0;python_version>="3.8"',
            'numpy==1.21.6;python_version<"3.8"',  # M1 is supported
            'numpy==1.22.4;python_version>="3.8"',
        ]
    },
)
