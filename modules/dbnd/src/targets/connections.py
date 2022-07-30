# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional


def build_conn_path(conn_type, hostname=None, port=None, path=None):
    # type: (str, Optional[str], Optional[int], Optional[str]) -> str
    return "{type}://{hostname}{port}/{path}".format(
        type=conn_type,
        hostname=hostname if hostname else "",
        port=":" + str(port) if port else "",
        path=path.strip("/") if path else "",
    )
