import os

from dbnd import output, pipeline, task
from targets import Target


def save_my_custom_object(path, data):
    with open(path, "w") as fd:
        fd.writelines(data)


def load_my_custom_object(path):
    with open(path, "r") as fd:
        return fd.readlines()


@task
def read(loc):
    # type: (Target) -> str
    data = load_my_custom_object(loc.path)
    return " ".join(data)


@task
def write(data, res=output.data.require_local_access):
    save_my_custom_object(res, data)


@pipeline
def write_read(data=["abcd", "zxcvb"]):
    path = write(data)
    r = read(path.res)
    return r


@task
def read_dir(loc):
    # type: (Target) -> str
    data = []
    for f in os.listdir(loc.path):
        data += load_my_custom_object(os.path.join(loc.path, f))
    return " ".join(data)


@task
def write_dir(data, res=output.data.require_local_access):
    os.mkdir(res)
    for i in range(3):
        file_path = os.path.join(res, str(i) + ".data")
        save_my_custom_object(file_path, data)


@pipeline
def write_read_dir(data=["abcd", "zxcvb"]):
    dir = write_dir(data)
    r = read_dir(dir.res)
    return r
