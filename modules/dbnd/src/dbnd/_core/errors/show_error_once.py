_SHOWN_ATTR = "_dbnd_shown"


def set_shown(ex):
    setattr(ex, _SHOWN_ATTR, True)


def is_shown(ex):
    if not ex:
        return False
    return getattr(ex, _SHOWN_ATTR, False)


def log_error(logger, ex, msg, *msg_args):
    if is_shown(ex):
        logger.error(msg, *msg_args)
    else:
        logger.exception(msg, *msg_args)
