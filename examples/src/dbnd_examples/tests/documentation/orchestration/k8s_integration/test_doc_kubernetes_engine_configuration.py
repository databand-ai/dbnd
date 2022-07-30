# © Copyright Databand.ai, an IBM Company 2022

#### DOC START

"""[kubernetes_cluster_env]
_from = local
remote_engine = kubernetes_engine"""


"""[kubernetes_engine]
_type = kubernetes

container_repository = databand_examples
container_tag =

docker_build = True
docker_build_push = True

namespace = databand

debug = False
secrets = [
   { "type":"env", "target": "AIRFLOW__CORE__SQL_ALCHEMY_CONN", "secret" : "databand-secret", "key": "airflow_sql_alchemy_conn"},
   { "type":"env", "target": "AIRFLOW__CORE__FERNET_KEY", "secret" : "databand-secret", "key": "airflow_fernet_key"},
   { "type":"env", "target": "DBND__CORE__DATABAND_URL", "secret" : "databand-secret", "key": "databand_url"}
   ]

pod_exit_code_to_retry_count = { 255:3 }
pod_default_retry_delay = 3m"""

"""secrets = [   { "type":"env", "target": "AWS_ACCESS_KEY_ID", "secret" : "aws-secrets" , "key" :"aws_access_key_id"},
          { "type":"env", "target": "AWS_SECRET_ACCESS_KEY", "secret" : "aws-secrets" , "key" :"aws_secret_access_key"}]"""


#### DOC END
