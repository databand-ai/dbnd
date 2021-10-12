from os import path

from setuptools import setup
from setuptools.config import read_configuration


BASE_PATH = path.join(path.dirname(path.abspath(__file__)))
CFG_PATH = path.join(BASE_PATH, "setup.cfg")
config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]
INSTALL_REQUIRES = [
    # we are still installing 'databand' in dockers.. "dbnd==" + version,
    "dbnd-airflow[airflow]==" + version,
    "scikit-learn==0.20.3;python_version<'3.5'",
    "scikit-learn==0.23.2;python_version>='3.5'",
    "scipy==1.1.0",
    "sklearn==0.0",
    "matplotlib==2.2.5;python_version<'3.5'",
    "matplotlib==3.3.0;python_version>='3.5'",
    "pandas<2.0.0,>=0.17.1",
]
setup(
    name="dbnd-examples",
    package_dir={"": "src"},
    version=version,
    zip_safe=False,
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    entry_points={"dbnd": ["dbnd-examples = dbnd_examples._plugin"]},
)
