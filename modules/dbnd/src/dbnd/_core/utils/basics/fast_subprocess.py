# © Copyright Databand.ai, an IBM Company 2022

from subprocess import CalledProcessError, call
from tempfile import NamedTemporaryFile


# based on https://stackoverflow.com/questions/13835055/python-subprocess-check-output-much-slower-then-call?lq=1
# standard check_output is superslow
def check_output(*popenargs, **kwargs):
    r"""Run command with arguments and return its output as a byte string.

    If the exit code was non-zero it raises a CalledProcessError.  The
    CalledProcessError object will have the return code in the returncode
    attribute and output in the output attribute.

    The arguments are the same as for the Popen constructor.  Example:
    """
    if "stdout" in kwargs:
        raise ValueError("stdout argument not allowed, it will be overridden.")

    with NamedTemporaryFile() as f:
        kwargs["stdout"] = f

        retcode = call(*popenargs, **kwargs)
        f.seek(0)
        output = f.read()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            raise CalledProcessError(retcode, cmd, output=output)
        return output
