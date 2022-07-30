# © Copyright Databand.ai, an IBM Company 2022

import sys

from dbnd import dbnd_tracking_start, task


@task
def some_function(x="my_default"):
    return x[0]


def my_f():
    some_function()
    some_function("my_value")


if __name__ == "__main__":
    dbnd_tracking_start()
    print("Main script is running")
    print("Command lines are: {} ", sys.argv)
    with open(sys.argv[1], "w") as fp:
        fp.write("Success")
    my_f()
    sys.exit(1)
