import logging
import os.path
import tarfile
import tempfile

from dbnd import output, parameter, task
from dbnd._core.constants import CloudType
from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.plugin.dbnd_plugins import assert_web_enabled
from dbnd._core.task import config
from dbnd._core.utils.timezone import utcnow
from targets import Target
from targets.types import Path


logger = logging.getLogger(__name__)


class DbSyncConfig(config.Config):
    """(Advanced) Databand's db sync behaviour"""

    _conf__task_family = "db_sync"

    export_root = parameter[Target]


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

            assert_web_enabled(reason="dbnd_web is required for export db")
            dbnd_context = get_databand_context()
            conn_string = dbnd_context.settings.web.get_sql_alchemy_conn()
            if conn_string.startswith("sqlite:"):
                from dbnd_web.utils.dbnd_db import get_sqlite_db_location

                db_file = get_sqlite_db_location(conn_string)
                logger.info("Exporting DB=%s", db_file)
                tar.add(db_file, arcname="dbnd.db")
            elif conn_string.startswith("postgresql"):
                with tempfile.NamedTemporaryFile(prefix="dbdump.", suffix=".sql") as tf:
                    from dbnd_web.utils.dbnd_db import dump_postgres

                    dump_postgres(conn_string, tf.name)
                    tar.add(tf.name, arcname="postgres-dbnd.sql")
            else:
                raise DatabandRuntimeError(
                    "Can not export db! "
                    "Currently, we support only sqlite and postgres db in automatic export"
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
