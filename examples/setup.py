# © Copyright Databand.ai, an IBM Company 2022

from os import path

from setuptools import setup
from setuptools.config import read_configuration


BASE_PATH = path.join(path.dirname(path.abspath(__file__)))
CFG_PATH = path.join(BASE_PATH, "setup.cfg")
config = read_configuration(CFG_PATH)

version = config["metadata"]["version"]

EXTRAS_REQUIRE = {"airflow": ["dbnd-airflow[airflow]==" + version]}

INSTALL_REQUIRES = [
    # we are still installing 'databand' in dockers.. "dbnd==" + version,
    "dbnd-airflow==" + version,
    "scikit-learn==0.23.2",
    'scipy==1.1.0;python_version<"3.8"',
    'scipy==1.8.0;python_version>="3.8"',
    "matplotlib==3.3.0",
    "pandas<2.0.0,>=0.17.1",
]

setup(
    name="dbnd-examples",
    package_dir={"": "src"},
    version=version,
    zip_safe=False,
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    entry_points={"dbnd": ["dbnd-examples = dbnd_examples._plugin"]},
)
