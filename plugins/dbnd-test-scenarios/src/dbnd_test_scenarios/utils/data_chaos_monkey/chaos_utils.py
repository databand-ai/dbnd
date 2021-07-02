import math

from random import randint, random


def chaos_int(value):
    value = int(value)
    return value + randint(math.ceil(-0.1 * value), math.ceil(0.1 * value))


def chaos_float(value):
    value = float(value)
    return value * (0.9 + 0.2 * random())


if __name__ == "__main__":
    print(
        chaos_int(200), chaos_int(0.5), chaos_int(15),
    )
    print(
        chaos_float(200), chaos_float(0.5), chaos_float(15),
    )
