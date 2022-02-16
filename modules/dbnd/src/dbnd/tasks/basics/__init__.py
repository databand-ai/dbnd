"""Module dbnd.tasks.basics contains some pipeline examples to start with."""
from dbnd.tasks.basics.sanity import dbnd_sanity_check
from dbnd.tasks.basics.shell import bash_cmd, bash_script
from dbnd.tasks.basics.simplest import SimplestPipeline, SimplestTask


__all__ = [
    "dbnd_sanity_check",
    "bash_cmd",
    "bash_script",
    "SimplestPipeline",
    "SimplestTask",
]
