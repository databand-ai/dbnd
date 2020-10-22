from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)

# A list of vendored packages
dbnd_vendors_list = [
    "cachetools",
    "hjson",
    "cloudpickle",
    "pendulum==1.4.4",
    "tabulate",
    "marshmallow==2.18.0",
    "croniter>=0.3.30,<0.4",
    "protobuf==3.13.0",
    "psutil>=4.2.0,<5.7.0",  # extracted use to vendorized_psutil.py
]


setuptools.setup(
    name="dbnd",
    package_dir={"": "src"},
    install_requires=[
        "tzlocal>=1.0.0,<2.0.0",
        "six",
        "more_itertools",
        "attrs",
        "pyyaml",  # yaml support in targets
        "pytz",  # python time zone for pendulum library
        "pytzdata",  # python time zone for pendulum library
        "requests>=2.18.0",  # API TRACKING
        "configparser<3.6.0,>=3.5.0",  # same versions as Airflow -- Moved library to _vendor
        "jinja2>=2.10.1, <2.11.0",  # same versions as Airflow
        "gitpython",
        'pygments<=2.5.2 ; python_version < "3.0"',
        'pygments>=2.6.1 ; python_version >= "3.0"',
        # backward compatible python
        'typing;python_version<"3.7"',  # Standalone pkg is incompatible with 3.7 and not required
        'pathlib2; python_version < "3.0"',  # pathlib support in python 2
        "pathlib2;python_version<='2.7'",
        "enum34;python_version<='2.7'",
        'contextlib2; python_version < "3"',
        "python-dateutil",
    ],
    extras_require={
        ':sys_platform=="win32"': ["colorama"],
        "tests": [
            "pandas==0.24.2",
            "numpy",
            "coverage",
            "pytest==4.5.0",  # 4.6.0 requires pluggy 0.12
            "pytest-cov==2.9.0",
            "pluggy==0.11.0",  # 0.12 has import_metadata, fails on py2
            "zope.interface",
            "mock",
            "pandas<2.0.0,>=0.17.1",  # airflow supports only this version
            "urllib3==1.23",  # otherwise we have 1.24 - conflict with 'requests'
            "tox==3.12.1",
            "matplotlib==2.2.5;python_version<'3.5'",
            "matplotlib==3.3.0;python_version>='3.5'",
            "tables==3.5.1",
            "feather-format",
            "pyarrow",
            "nbconvert",
            "nbformat",
            "jupyter",
            "traitlets>=4.2,<5.0.0",  # required by jupyter, fix py37 compatibility
            "IPython>=4.0.0, <7.0",
            "jupyter_contrib_nbextensions",
            "idna<=2.7",  # conflict with requests (require 2.8 <)
            # conflict with pandas version on new openpyxl: got invalid input value of type <class 'xml.etree.ElementTree.Element'>, expected string or Element
            "openpyxl==2.6.4",
            "sklearn",
            "WTForms<2.3.0",  # fixing ImportError: cannot import name HTMLString at 2.3.0
            "wheel",  # for fat_wheel tests
        ],
    },
    entry_points={"console_scripts": ["dbnd = dbnd:dbnd_main"]},
)
