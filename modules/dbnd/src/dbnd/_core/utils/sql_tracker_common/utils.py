from collections import defaultdict
from typing import Union


def ddict2dict(d: Union[defaultdict, dict]) -> dict:
    """convert defaultdict into a dict"""
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = ddict2dict(v)
    return dict(d)


def strip_quotes(command: str) -> str:
    if command:
        return command.replace("'", "").replace('"', "")


def get_redshift_uri(host: str, db: str, schema: str, table: str) -> str:
    return f"redshift://{host}/{db}/{schema}/{table}".lower()
