# ORIGIN: https://github.com/databricks/mlflow
import logging
import os
import tarfile

from subprocess import PIPE, STDOUT, Popen
from sys import version_info

from dbnd.errors import DatabandError


def run_commands(commands, work_path=None):
    for c in commands:
        if work_path:
            proc = Popen(
                c.split(),
                cwd=work_path,
                stdout=PIPE,
                stderr=STDOUT,
                universal_newlines=True,
            )
        else:
            proc = Popen(c.split(), stdout=PIPE, stderr=STDOUT, universal_newlines=True)
        proc.wait()
        logging.info("".join(proc.stdout.readlines()))
        if proc.returncode != 0:
            raise DatabandError(
                "A child process '{command}' exited with non zero return code: {code}".format(
                    command="".join(c), code=proc.returncode
                )
            )


def get_unique_resource_id(max_length=None):
    """
    Obtains a unique id that can be included in a resource name. This unique id is a valid
    DNS subname.

    :param max_length: The maximum length of the identifier
    :return: A unique identifier that can be appended to a user-readable resource name to avoid
             naming collisions.
    """
    import uuid
    import base64

    if max_length is not None and max_length <= 0:
        raise ValueError(
            "The specified maximum length for the unique resource id must be positive!"
        )

    uuid_bytes = uuid.uuid4().bytes
    # Use base64 encoding to shorten the UUID length. Note that the replacement of the
    # unsupported '+' symbol maintains uniqueness because the UUID byte string is of a fixed,
    # 16-byte length
    uuid_b64 = base64.b64encode(uuid_bytes)
    if version_info >= (3, 0):
        # In Python3, `uuid_b64` is a `bytes` object. It needs to be
        # converted to a string
        uuid_b64 = uuid_b64.decode("ascii")
    unique_id = uuid_b64.rstrip("=\n").replace("/", "-").replace("+", "AB").lower()
    if max_length is not None:
        unique_id = unique_id[: int(max_length)]
    return unique_id


def make_tarfile(output_filename, source_dir):
    """
    create a tar.gz from a directory.
    """
    with tarfile.open(output_filename, "w:gz") as tar:
        for f in os.listdir(source_dir):
            tar.add(os.path.join(source_dir, f), arcname=f)
