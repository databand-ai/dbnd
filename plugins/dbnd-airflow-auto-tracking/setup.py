import setuptools


setuptools.setup(
    name="dbnd-airflow-auto-tracking",
    package_dir={"": "src"},
    # we are not requiring airflow, as this plugin should be installed into existing airflow deployment
    install_requires=["dbnd"],
    entry_points={
        "airflow.plugins": [
            "dbnd_airflow_auto_tracking = dbnd_airflow_auto_tracking.dbnd_airflow_auto_tracking:DbndAutoTracking"
        ]
    },
)
