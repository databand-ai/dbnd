# © Copyright Databand.ai, an IBM Company 2022

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
    "psutil>=4.2.0,<5.7.0",  # extracted use to vendorized_psutil.py
]

setuptools.setup(
    name="dbnd",
    package_dir={"": "src"},
    python_requires=">=3.6, <3.13",
    install_requires=[
        "tzlocal",
        "six",
        "more-itertools",
        "attrs!=21.1.0",  # yanked version, breaks dbnd
        "pyyaml",  # yaml support in targets
        "pytz",  # python time zone for pendulum library
        "pytzdata",  # python time zone for pendulum library
        "requests>=2.18.0",  # API TRACKING
        'configparser<3.6.0,>=3.5.0; python_version<"3.10"',  # same versions as Airflow -- Moved library to _vendor
        "pygments>=2.6.1",
        # backward compatible python
        'typing;python_version<"3.7"',  # Standalone pkg is incompatible with 3.7 and not required
        "python-dateutil",
        "sqlparse",
        "jinja2",
        # WARNING! Keep install_requires in sync with dbnd.requirements.txt file:
        # use dbnd.requirements.txt file generated by `make dist-python`
    ],
    extras_require={
        ':sys_platform=="win32"': ["colorama"],
        "tests": [
            "coverage",
            "tox==3.12.1",
            'pytest==4.5.0;python_version<"3.10"',  # 4.6.0 requires pluggy 0.12
            'pytest-cov==2.9.0;python_version<"3.10"',
            'pytest==6.2.5;python_version>="3.10"',
            'pytest-cov==3.0.0;python_version>="3.10"',
            "mock",
            "wheel",  # for fat_wheel tests
        ],
        "test-pandas": [
            "openpyxl==2.6.4",
            'numpy<1.23;python_version<"3.12"',
            'numpy<2;python_version>="3.12"',
            'pandas<2.0.0,>=0.17.1;python_version<"3.12"',  # airflow supports only this version
            'pandas>2;python_version>="3.12"',
            'scikit-learn==0.23.2;python_version<"3.8"',
            'scikit-learn==1.2.0;python_version >= "3.8" and python_version < "3.12"',
            'scikit-learn==1.5.0;python_version>="3.12"',
            'matplotlib==3.3.0;python_version<"3.8"',
            'matplotlib==3.6.2;python_version>="3.8"',
            'tables==3.7.0;python_version<"3.12"',
            'tables==3.9.2;python_version>="3.12"',
            "feather-format",
            "pyarrow",
        ],
        "test-spark2": ["pytest-spark==0.6.0", 'pyspark==2.4.4;python_version<"3.8"'],
        "test-spark3": ["pytest-spark==0.6.0", 'pyspark==3.3.1;python_version>="3.8"'],
        "jupyter": [
            "qtconsole==4.7.7",
            "nbconvert",
            "nbformat",
            "jupyter",
            "traitlets>=4.2,<5.0.0",  # required by jupyter, fix py37 compatibility
            "IPython",
            "jupyter_contrib_nbextensions",
        ],
    },
    entry_points={"console_scripts": ["dbnd = dbnd:dbnd_main"]},
)
