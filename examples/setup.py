from os import path

from setuptools import setup
from setuptools.config import read_configuration


BASE_PATH = path.join(path.dirname(path.abspath(__file__)))
CFG_PATH = path.join(BASE_PATH, "setup.cfg")
config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]
INSTALL_REQUIRES = [
    # we are still installing 'databand' in dockers.. "dbnd==" + version,
    "matplotlib",
    "scikit-learn;python_version>='3.5'",
    "scikit-learn==0.20.3;python_version<'3.5'",  # The latest version which supports Python 2.7
    "scipy==1.1.0",
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
