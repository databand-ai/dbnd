import setuptools


setuptools.setup(
    name="dbnd",
    package_dir={"": "src"},
    install_requires=[
        "tzlocal>=1.0.0,<2.0.0",
        "six",
        "more_itertools",
        # "cachetools", -- Moved library to _vendor
        "attrs",
        # "hjson", -- Moved library to _vendor
        "pyyaml",  # yaml support in targets
        "pytz",  # python time zone for pendulum library
        "pytzdata",  # python time zone for pendulum library
        # "cloudpickle",  # serializing pipelines -- Moved library to _vendor
        "requests>=2.18.0",  # API TRACKING
        # "pendulum==1.4.4", -- Moved library to _vendor
        "configparser<3.6.0,>=3.5.0",  # same versions as Airflow -- Moved library to _vendor
        # "tabulate", -- Moved library to _vendor
        # "marshmallow==2.18.0", -- Moved library to _vendor
        "jinja2>=2.10.1, <2.11.0",  # same versions as Airflow
        "gitpython",
        "pandas<1.0.0,>=0.17.1",
        "numpy",
        "pygments",
        # backward compatible python
        'typing;python_version<"3.7"',  # Standalone pkg is incompatible with 3.7 and not required
        'pathlib2; python_version < "3.0"',  # pathlib support in python 2
        "pathlib2;python_version<='2.7'",
        "enum34;python_version<='2.7'",
        'contextlib2; python_version < "3"',
        # "croniter>=0.3.30,<0.4", -- Moved library to _vendor
        # "psutil>=4.2.0,<5.7.0", -- Extracted use to vendorized_psutil.py
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
            "WTForms<2.3.0",  # fixing ImportError: cannot import name HTMLString at 2.3.0
        ],
    },
    entry_points={"console_scripts": ["dbnd = dbnd:dbnd_main"]},
)
