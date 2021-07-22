"""

import pandas as pd

import module1
import module2

#### DOC START
# module1.py
from dbnd import task, track_functions, track_module_functions
from module1 import calculate_alpha, calculate_beta, calculate_coefficient


# define a function with a decorator


@task
def prepare_data(data: pd.DataFrame, length: int):
    return data


# module1.py
def calculate_alpha():
    pass


def calculate_beta():
    pass


def calculate_coefficient():
    pass


# module2.py


track_functions(calculate_alpha, calculate_beta, calculate_coefficient)


def calculate_all():
    calculate_alpha()
    calculate_beta()
    calculate_coefficient()


# module2.py


track_module_functions(module1)


def calculate_all():
    calculate_alpha()
    calculate_beta()
    calculate_coefficient()


# module3.py


track_modules(module1, module2)


def f5():
    module2.calculate_all()


#### DOC END

"""
