# © Copyright Databand.ai, an IBM Company 2022

import logging
import os.path
import subprocess
import tarfile
import tempfile

from dbnd import output, task
from dbnd._core.constants import CloudType
from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.utils.timezone import utcnow
from targets.types import Path


logger = logging.getLogger(__name__)


@task(archive=output(output_ext=".tar.gz")[Path])
def export_db(
    archive,
    include_db=True,
    include_logs=True,
    task_version=utcnow().strftime("%Y%m%d_%H%M%S"),
):
    # type: (Path, bool, bool, str)-> None

    from dbnd._core.current import get_databand_context

    logger.info("Compressing files to %s..." % archive)
    with tarfile.open(str(archive), "w:gz") as tar:

        if include_db:
            dbnd_context = get_databand_context()
            conn_string = dbnd_context.config.get("webserver", "sql_alchemy_conn")
            if conn_string.startswith("postgresql"):
                with tempfile.NamedTemporaryFile(prefix="dbdump.", suffix=".sql") as tf:
                    dump_postgres(conn_string, tf.name)
                    tar.add(tf.name, arcname="postgres-dbnd.sql")
            else:
                raise DatabandRuntimeError(
                    "Can not export db! "
                    "Currently, we support only postgres db in automatic export"
                )

        if include_logs:
            context = get_databand_context()
            local_env = context.settings.get_env_config(CloudType.local)
            logs_folder = local_env.dbnd_local_root.folder("logs").path
            if os.path.exists(logs_folder):
                logger.info("Adding run folder from '%s'", logs_folder)
                tar.add(logs_folder, "run")
            else:
                logger.warning("Logs dir '%s' is not found", logs_folder)


def dump_postgres(conn_string, dump_file):
    logger.info(
        "backing up postgres DB to %s, pg_dump and sqlalchemy are required!", dump_file
    )

    from sqlalchemy.engine.url import make_url

    url = make_url(conn_string)

    cmd = [
        "pg_dump",
        "-h",
        url.host,
        "-p",
        str(url.port),
        "-U",
        url.username,
        "-Fc",
        "-f",
        dump_file,
        "-d",
        url.database,
    ]
    logger.info("Running command: %s", subprocess.list2cmdline(cmd))
    env = os.environ.copy()
    env["PGPASSWORD"] = url.password
    subprocess.check_call(args=cmd, env=env)
