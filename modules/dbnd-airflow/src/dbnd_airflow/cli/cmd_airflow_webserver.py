from __future__ import print_function

import logging
import os
import signal
import subprocess
import sys
import textwrap
import time

import psutil

from airflow import configuration as config, settings
from airflow.bin.cli import restart_workers, setup_logging
from airflow.www_rbac.app import cached_app
from daemon import DaemonContext
from daemon.pidfile import TimeoutPIDLockFile

from dbnd._core.errors import DatabandSystemError
from dbnd._core.settings.core import WebserverConfig
from dbnd._core.utils.basics import ascii_art
from dbnd._core.utils.platform import windows_compatible_mode
from dbnd._vendor import click
from dbnd_airflow.cli.cmd_scheduler import link_dropin_dbnd_scheduled_dags
from dbnd_airflow.web.airflow_app import create_app


log = logging.getLogger(__name__)


def conf(name):
    def _load():
        from airflow import configuration as conf

        return conf.get("webserver", name) or None

    return _load


@click.command()
@click.option(
    "--port", "-p", type=click.IntRange(1024, 65535), default=conf("web_server_port")
)
@click.option("--workers", type=click.IntRange(1, 64), default=conf("workers"))
@click.option(
    "--workerclass",
    type=click.Choice(["sync", "eventlet", "gevent", "tornado"]),
    default=conf("worker_class"),
)
@click.option("--worker-timeout", type=int, default=conf("web_server_worker_timeout"))
@click.option("--hostname", default=conf("web_server_host"))
@click.option("--pid", type=click.Path())
@click.option("--daemon", "-D", is_flag=True)
@click.option("--stdout", type=click.Path())
@click.option("--stderr", type=click.Path())
@click.option("--log-file", "-l", type=click.Path())
@click.option(
    "--access-logfile", "-A", type=click.Path(), default=conf("access_logfile")
)
@click.option("--error-logfile", "-E", type=click.Path(), default=conf("error_logfile"))
@click.option(
    "--ssl-cert", type=click.Path(exists=True), default=conf("web_server_ssl_cert")
)
@click.option(
    "--ssl-key", type=click.Path(exists=True), default=conf("web_server_ssl_key")
)
@click.option("--debug", "-d", is_flag=True)
def airflow_webserver(
    port,
    workers,
    workerclass,
    worker_timeout,
    hostname,
    pid,
    daemon,
    stdout,
    stderr,
    log_file,
    access_logfile,
    error_logfile,
    ssl_cert,
    ssl_key,
    debug,
):
    """
    Start Airflow's Web Server
    """

    click.echo(ascii_art.big)

    # make sure the scheduler drop-in is linked so that scheduled jobs are available for the airflow ui
    # however don't try to re-link it if it's already there to not interfere with the scheduler that could
    # be running on the same machine
    link_dropin_dbnd_scheduled_dags(settings.DAGS_FOLDER, unlink_first=False)

    webserver_config = WebserverConfig()
    working_dir = webserver_config.working_dir

    if not ssl_cert and ssl_key:
        raise DatabandSystemError(
            "An SSL certificate must also be provided for use with " + ssl_key
        )

    if ssl_cert and not ssl_key:
        raise DatabandSystemError(
            "An SSL key must also be provided for use with " + ssl_cert
        )

    if debug:
        click.echo(
            "Starting the web server on port {0} and host {1}.".format(port, hostname)
        )
        app, _ = create_app()
        app.run(
            debug=True,
            port=port,
            host=hostname,
            ssl_context=(ssl_cert, ssl_key) if ssl_cert and ssl_key else None,
        )

    elif windows_compatible_mode:
        click.echo(
            "Starting the web server on port {0} and host {1}. You are running windows compatible mode!".format(
                port, hostname
            )
        )
        app, _ = create_app()
        app.run(
            debug=False,
            port=port,
            host=hostname,
            ssl_context=(ssl_cert, ssl_key) if ssl_cert and ssl_key else None,
        )

    else:
        app = cached_app()

        stderr = stderr or os.path.join(working_dir, "airflow-webserver.err")
        stdout = stdout or os.path.join(working_dir, "airflow-webserver.out")
        log_file = log_file or os.path.join(working_dir, "airflow-webserver.log")

        # if need this line , we need to update all deployed docker processes
        # as they are looking for this file at health check
        pid = pid or os.path.join(working_dir, "airflow-webserver.pid")

        print(
            textwrap.dedent(
                """\
                Running the Gunicorn Server with:
                Workers: {workers} {workerclass}
                Host: {hostname}:{port}
                Timeout: {worker_timeout}
                Logfiles: {log_file} {stdout} {stderr}
                =================================================================\
            """.format(
                    **locals()
                )
            )
        )

        run_args = [
            "gunicorn",
            "-w",
            str(workers),
            "-k",
            str(workerclass),
            "-t",
            str(worker_timeout),
            "-b",
            hostname + ":" + str(port),
            "-n",
            "airflow-webserver",
            "-p",
            str(pid),
            # "-c",
            # "python:dbnd_airflow.gunicorn_config",
        ]

        if access_logfile:
            run_args += ["--access-logfile", str(access_logfile)]

        if error_logfile:
            run_args += ["--error-logfile", str(error_logfile)]

        if daemon:
            run_args += ["-D"]

        if ssl_cert:
            run_args += ["--certfile", ssl_cert, "--keyfile", ssl_key]

        run_args += ["dbnd_airflow.web.airflow_app:create_gunicorn_app()"]

        gunicorn_master_proc = None

        def kill_proc(dummy_signum, dummy_frame):
            gunicorn_master_proc.terminate()
            gunicorn_master_proc.wait()
            sys.exit(0)

        def monitor_gunicorn(gunicorn_master_proc):
            # These run forever until SIG{INT, TERM, KILL, ...} signal is sent
            if config.getint("webserver", "worker_refresh_interval") > 0:
                master_timeout = config.getint("webserver", "web_server_master_timeout")
                restart_workers(gunicorn_master_proc, workers, master_timeout)
            else:
                while True:
                    time.sleep(1)

        if daemon:
            base, ext = os.path.splitext(pid)

            handle = setup_logging(log_file)
            stdout = open(stdout, "w+")
            stderr = open(stderr, "w+")

            ctx = DaemonContext(
                pidfile=TimeoutPIDLockFile(base + "-monitor" + ext, -1),
                files_preserve=[handle],
                stdout=stdout,
                stderr=stderr,
                signal_map={signal.SIGINT: kill_proc, signal.SIGTERM: kill_proc},
            )
            with ctx:
                subprocess.Popen(run_args, close_fds=True)

                # Reading pid file directly, since Popen#pid doesn't
                # seem to return the right value with DaemonContext.
                while True:
                    try:
                        with open(ctx.pidfile) as f:
                            gunicorn_master_proc_pid = int(f.read())
                            break
                    except IOError:
                        log.debug("Waiting for gunicorn's pid file to be created.")
                        time.sleep(0.1)

                gunicorn_master_proc = psutil.Process(gunicorn_master_proc_pid)
                monitor_gunicorn(gunicorn_master_proc)

            stdout.close()
            stderr.close()
        else:
            log.info("gunicorn cmd: %s" % " ".join(run_args))
            gunicorn_master_proc = subprocess.Popen(run_args, close_fds=True)

            signal.signal(signal.SIGINT, kill_proc)
            signal.signal(signal.SIGTERM, kill_proc)

            monitor_gunicorn(gunicorn_master_proc)
