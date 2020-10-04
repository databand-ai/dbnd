import errno
import logging
import os


logger = logging.getLogger(__name__)

try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit


def trailing_slash(path):
    # I suppose one day schema-like paths, like
    # file:///path/blah.txt?params=etc can be parsed too
    return path[-1] if path[-1] in r"\/" else ""


def no_trailing_slash(path):
    if not path:
        return path
    return path[:-1] if path[-1] in r"\/" else path


def safe_mkdirs(path, mode):
    """
    safe version of make dir
    """

    o_umask = os.umask(0)
    try:
        os.makedirs(path, mode)
    except OSError as e:
        if e.errno != errno.EEXIST:
            logger.error("Could not create dir: %s. %s", path, e)
            raise
    finally:
        os.umask(o_umask)


def path_to_bucket_and_key(path):
    (scheme, netloc, path, _, _) = urlsplit(str(path))
    path_without_initial_slash = path[1:]
    return netloc, path_without_initial_slash
