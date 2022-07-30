# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.current import get_databand_context


def kill_run(run_uid, ctx=None):
    ctx = ctx or get_databand_context()
    return ctx.databand_api_client.api_request(endpoint="runs/stop", data=[run_uid])
