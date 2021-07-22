from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-gcp",
    package_dir={"": "src"},
    install_requires=[
        "dbnd==" + version,
        "httplib2>=0.9.2",
        "google-api-python-client>=1.6.0, <2.0.0dev",
        "google-auth>=1.0.0, <2.0.0dev",
        "google-auth-httplib2>=0.0.1",
        "google-cloud-container>=0.1.1",
        "PyOpenSSL",
        "pandas-gbq",
    ],
    extras_require=dict(tests=["dbnd_test_scenarios==" + version]),
    entry_points={"dbnd": ["dbnd-gcp = dbnd_gcp._plugin"]},
)
