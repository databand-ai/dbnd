# © Copyright Databand.ai, an IBM Company 2022

import datetime
import json

import airflow

from flask import Response

from dbnd._vendor import version


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        # convert dates and numpy objects in a json serializable format
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def json_response(obj):
    return Response(
        response=json.dumps(obj, indent=4, cls=JsonEncoder),
        status=200,
        mimetype="application/json",
    )


# we can not use `dbnd_airflow.compat` here,
# as we have .compat usage at "import" level,
# meaning we might have circular dependency while loading airflow plugins
# (import dbnd_airflow.compat -> import airflow -> import plugin -> import dbnd_airflow.compat again)
AIRFLOW_VERSION_2 = version.parse(airflow.version.version) >= version.parse("2.0.0")
AIRFLOW_VERSION_BEFORE_2_2 = version.parse(airflow.version.version) < version.parse(
    "2.2.0"
)
