# © Copyright Databand.ai, an IBM Company 2022

#### DOC START

"""[kubernetes_engine] # make sure it's your engine name

secrets = [
   { "type":"env", "target": "AIRFLOW__CORE__SQL_ALCHEMY_CONN", "secret" : "databand-secret", "key": "airflow_sql_alchemy_conn"},
   { "type":"env", "target": "AIRFLOW__CORE__FERNET_KEY", "secret" : "databand-secret", "key": "airflow_fernet_key"},
   { "type":"env", "target": "DBND__CORE__DATABAND_URL", "secret" : "databand-secret", "key": "databand_url"}
   ]

"""

#### DOC END
