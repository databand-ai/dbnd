# © Copyright Databand.ai, an IBM Company 2022

# patch windows module so python-daemon will work on windows
# this is required for python-daemon<2.2 imported by airflow 1.10.1.
# This issue is resolved in newer versions of python-daemon


def getpwuid(*args, **kwargs):
    return -1
