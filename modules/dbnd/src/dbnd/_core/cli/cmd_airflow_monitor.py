from time import sleep

import pendulum

import requests

from dbnd._vendor import click


class _Obj(object):
    pass


def _to_obj(d):
    # it's really dirty for now, but quick... :( TODO: implement correctly
    if isinstance(d, dict):
        o = _Obj()
        for key, value in d.items():
            if value and key in (
                "start_date",
                "end_date",
                "execution_date",
                "timestamp",
            ):
                val = pendulum.parse(value)
            else:
                val = _to_obj(value)
            setattr(o, key, val)
        return o
    if isinstance(d, list):
        return [_to_obj(i) for i in d]
    return d


@click.command()
@click.option("--interval", type=click.INT)
@click.option("--url", type=click.STRING)
def airflow_monitor(interval, url):
    """Start Airflow Data Importer"""
    from dbnd import new_dbnd_context
    from dbnd._core.tracking.airflow_sync import do_import_data
    from dbnd._core.tracking.airflow_sync.converters import to_export_data

    if not url:
        url = "http://localhost:8080/admin/data_export_plugin/export_data"
    if not interval:
        interval = 10
    since = None

    with new_dbnd_context() as dc:
        while True:
            params = {}
            if since:
                params["since"] = since.isoformat()
            data = requests.get(url, params=params)
            export_data = to_export_data(_to_obj(data.json()), since)
            do_import_data(export_data)
            since = export_data.timestamp
            sleep(interval)
