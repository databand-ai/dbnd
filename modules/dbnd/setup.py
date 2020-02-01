import setuptools


setuptools.setup(
    name="dbnd",
    package_dir={"": "src"},
    install_requires=[
        "tzlocal>=1.0.0,<2.0.0",
        "six",
        "more_itertools",
        "cachetools",
        "attrs",
        "hjson",
        "pyyaml",  # yaml support in targets
        "cloudpickle",  # serializing pipelines
        "requests>=2.18.0",  # API TRACKING
        "pendulum==1.4.4",
        "configparser<3.6.0,>=3.5.0",  # same versions as Airflow
        "tabulate",
        "marshmallow==2.18.0",
        "jinja2>=2.10.1, <2.11.0",  # same versions as Airflow
        "gitpython",
        "pandas",
        "numpy",
        "pygments",
        # backward compatible python
        'typing;python_version<"3.7"',  # Standalone pkg is incompatible with 3.7 and not required
        'pathlib2; python_version < "3.0"',  # pathlib support in python 2
        "pathlib2;python_version<='2.7'",
        "enum34;python_version<='2.7'",
        'contextlib2; python_version < "3"',
        "croniter>=0.3.30,<0.4",
        "psutil>=5.6.7,<6.0.0",
    ],
    extras_require={
        ':sys_platform=="win32"': ["colorama"],
        "tests": [
            "coverage",
            "pytest==4.5.0",  # 4.6.0 requires pluggy 0.12
            "pytest-cov",
            "pluggy==0.11.0",  # 0.12 has import_metadata, fails on py2
            "zope.interface",
            "mock",
            "pandas<1.0.0,>=0.17.1",  # airflow supports only this version
            "urllib3==1.23",  # otherwise we have 1.24 - conflict with 'requests'
            "tox==3.12.1",
            "matplotlib",
            "tables==3.5.1",
            "feather-format",
            "pyarrow",
            "nbconvert",
            "nbformat",
            "jupyter",
            "IPython>=4.0.0, <7.0",
            "jupyter_contrib_nbextensions",
            "idna<=2.7",  # conflict with requests (require 2.8 <)
            # conflict with pandas version on new openpyxl: got invalid input value of type <class 'xml.etree.ElementTree.Element'>, expected string or Element
            "openpyxl==2.6.4",
            "sklearn",
        ],
    },
    entry_points={"console_scripts": ["dbnd = dbnd:dbnd_main"]},
)
