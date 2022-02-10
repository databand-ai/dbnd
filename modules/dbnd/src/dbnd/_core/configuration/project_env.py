import os
import sys


def _is_init_mode():
    from subprocess import list2cmdline

    cmdline = list2cmdline(sys.argv)
    return (
        "dbnd project-init" in cmdline
        or cmdline == "-c project-init"
        or " project-init" in cmdline
        # backward compatibility
        or "dbnd init_project" in cmdline
        or cmdline == "-c init_project"
    )


def _is_running_airflow_webserver():
    # TODO: ...
    if not sys.argv or len(sys.argv) < 2:
        return False

    if sys.argv[0].endswith("airflow") and sys.argv[1] == "webserver":
        return True

    if sys.argv[0].endswith("gunicorn") and "airflow-webserver" in sys.argv:
        return True

    return False


def _init_windows_python_path(databand_package):
    # patch pwd and resource system modules
    if os.name == "nt":
        sys.path.insert(
            0, os.path.join(databand_package, "utils", "platform", "windows_compatible")
        )
