import random
import string


def random_text(n=10):
    return "".join(random.choice(string.ascii_letters) for _ in range(n))
