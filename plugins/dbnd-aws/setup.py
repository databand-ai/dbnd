from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-aws",
    package_dir={"": "src"},
    install_requires=[
        "dbnd==" + version,
        # otherwise airflow dependencies are broken
        "httplib2>=0.9.2",
        "boto3",
        "botocore",
        "s3fs",
    ],
    extras_require={
        "tests": [
            "awscli",
            "WTForms<2.3.0",  # fixing ImportError: cannot import name HTMLString at 2.3.0
            "dbnd_test_scenarios==" + version,
        ]
    },
    entry_points={"dbnd": ["dbnd-aws = dbnd_aws._plugin"]},
)
