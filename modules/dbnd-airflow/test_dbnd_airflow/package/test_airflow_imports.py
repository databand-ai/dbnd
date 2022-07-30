# © Copyright Databand.ai, an IBM Company 2022


def test_import_databand():
    print("Starting Import")
    import dbnd

    str(dbnd)


def test_import_airflow_settings():
    print("Starting Import")
    import airflow.settings

    str(airflow.settings)
