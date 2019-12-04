import uuid


def get_uuid():
    # TODO: obfuscate getnode() - mac address part
    return uuid.uuid1()
