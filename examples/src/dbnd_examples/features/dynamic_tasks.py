import logging

from typing import Tuple

from dbnd import task


@task
def say_hello(text="sdfsd"):
    greeting = "Hey, {}!".format(text)
    logging.info(greeting)
    return greeting


@task
def join_greeting(base_greeting, extra_name):
    return "{} and {}".format(base_greeting, extra_name)


@task
def say_hello_pipe(users_num=3):
    v = say_hello("some_user")
    for i in range(users_num):
        v = join_greeting(v, "user {}".format(i))

    return v


@task
def say_hello_to_everybody(users_num=3) -> Tuple[str, str]:
    v = ""
    for i in range(users_num):
        v = say_hello("user {}".format(i))

    hello_pipe = say_hello_pipe()
    return v, hello_pipe


if __name__ == "__main__":
    say_hello_to_everybody()
