from dbnd import dbnd_handle_errors, task


@task
def func_you_want_to_marry_with(numerator=1):
    # type: ( datetime.datetime)-> str
    dividing_by_zero_is_fun = numerator / 0
    return "We'll definitely get here: %s" % dividing_by_zero_is_fun


def main_1():
    # Test number 1 - bad parameter, error on build:
    with dbnd_handle_errors(False) as eh:
        func_you_want_to_marry_with.task(bad_param="This param does not exist")


def main_2():
    # # Test number 2 - good parameter, error in runtime
    with dbnd_handle_errors(True) as eh:
        func_you_want_to_marry_with.dbnd_run(numerator=42)


if __name__ == "__main__":
    main_1()
