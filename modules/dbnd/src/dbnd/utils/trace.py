import uuid

from contextlib import contextmanager


TRACING_ID = uuid.uuid4()


@contextmanager
def new_tracing_id():
    global TRACING_ID
    prev_identifier = TRACING_ID
    TRACING_ID = uuid.uuid4()
    try:
        yield TRACING_ID
    finally:
        TRACING_ID = prev_identifier


def get_tracing_id():
    return TRACING_ID
