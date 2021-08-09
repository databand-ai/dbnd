from typing import Tuple


_TERMINAL_SIZE = None


def get_terminal_size():
    # type: ()->Tuple[int,int]
    global _TERMINAL_SIZE
    if _TERMINAL_SIZE:
        return _TERMINAL_SIZE
    try:
        from click import get_terminal_size as click_terminal_size

        _TERMINAL_SIZE = click_terminal_size()
    except Exception:
        _TERMINAL_SIZE = (140, 25)
    return _TERMINAL_SIZE
