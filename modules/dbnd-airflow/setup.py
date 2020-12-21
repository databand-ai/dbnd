from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

requirements_for_airflow = [
    "WTForms<2.3.0",  # fixing ImportError: cannot import name HTMLString at 2.3.0
    "Werkzeug<1.0.0,>=0.15.0",
    "cattrs==1.0.0",  # airflow requires ~0.9 but it's py2 incompatible (bug)
    "psycopg2>=2.7.4,<2.8",
]

setuptools.setup(
    name="dbnd-airflow",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "packaging", "psycopg2>=2.7.4,<2.8"],
    extras_require=dict(
        airflow_1_10_7=requirements_for_airflow + ["apache-airflow==1.10.7"],
        airflow_1_10_8=requirements_for_airflow + ["apache-airflow==1.10.8"],
        airflow_1_10_9=requirements_for_airflow + ["apache-airflow==1.10.9"],
        airflow_1_10_10=requirements_for_airflow + ["apache-airflow==1.10.10"],
        airflow_1_10_11=requirements_for_airflow + ["apache-airflow==1.10.11"],
        airflow_1_10_12=requirements_for_airflow + ["apache-airflow==1.10.12"],
        airflow=requirements_for_airflow + ["apache-airflow==1.10.9"],
        tests=[
            # airflow support
            "pandas<2.0.0,>=0.17.1",
            # azure
            "azure-storage-blob",
            # aws
            "httplib2>=0.9.2",
            "boto3<=1.15.18",
            "s3fs",
            # gcs
            "httplib2>=0.9.2",
            "google-api-python-client>=1.6.0, <2.0.0dev",
            "google-auth>=1.0.0, <2.0.0dev",
            "google-auth-httplib2>=0.0.1",
            "google-cloud-container>=0.1.1",
            "PyOpenSSL",
            "pandas-gbq",
            # docker
            "docker~=3.0",
            # k8s
            "kubernetes==9.0.0",
            "cryptography>=2.0.0",
            "WTForms<2.3.0",  # fixing ImportError: cannot import name HTMLString at 2.3.0
            "dbnd_test_scenarios==" + version,
        ],
    ),
    entry_points={
        "console_scripts": [
            "dbnd-airflow = dbnd_airflow.dbnd_airflow_main:main",
            "dbnd-airflow-create-api-user = dbnd_airflow.utils:create_user",
        ],
        "dbnd": ["dbnd-airflow = dbnd_airflow._plugin"],
    },
)
