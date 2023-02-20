# © Copyright Databand.ai, an IBM Company 2022

"""
Used by shell scripts to get current token
 logging - stderr
 result - stdout
"""

import json
import sys

from airflow.models import Connection
from airflow.utils import db


def get_token_from_config():
    with db.create_session() as session:
        connection = (
            session.query(Connection)
            .filter(Connection.conn_id == "dbnd_config")
            .first()
        )
        if not connection:
            print("No dbnd_config", file=sys.stderr)
            sys.exit(-1)

        token = connection.get_extra()
        if not token:
            print("Connection extra dbnd_config is empty", file=sys.stderr)
            return None
        token = json.loads(token)

        print("token=%s in dbnd_config" % token, file=sys.stderr)
        token = token.setdefault("core", {}).get("databand_access_token")
        if not token:
            print("no token in config", file=sys.stderr)
            return None
        return token


if __name__ == "__main__":
    token = get_token_from_config()
    if not token:
        print("Can't find token", file=sys.stderr)
        sys.exit(-1)
    print(token)
