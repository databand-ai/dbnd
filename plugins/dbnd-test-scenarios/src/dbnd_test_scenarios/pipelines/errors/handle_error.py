from dbnd import dbnd_handle_errors, task


@task
def func_with_error(numerator=1):
    # type: ( datetime.datetime)-> str
    dividing_by_zero_is_fun = numerator / 0
    return "We'll definitely get here: %s" % dividing_by_zero_is_fun


def main_1():
    # Test number 1 - bad parameter, error on build:
    with dbnd_handle_errors(False) as eh:
        func_with_error.task(bad_param="This param does not exist")


def main_2():
    # # Test number 2 - good parameter, error in runtime
    with dbnd_handle_errors(True) as eh:
        func_with_error.dbnd_run(numerator=42)


if __name__ == "__main__":
    main_1()
