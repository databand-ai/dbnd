from six.moves.urllib.parse import (
    parse_qsl,
    quote_plus,
    unquote_plus,
    urlencode,
    urlparse,
    urlunparse,
)


WEB2DBND_MAP = {"http": "dbnd", "https": "dbnd+s"}

DBND2WEB_MAP = {
    "dbnd": "http",
    "databand": "http",
    "dbnd+s": "https",
    "databand+s": "https",
}


def is_composite_uri(uri):
    """Check if the passed uri has a dbnd store uri schema

    Parameters:
        uri (str): uri to be checked
    Returns:
        bool: is the uri has a dbnd store uri schema

    >>> is_composite_uri('dbnd://localhost:8080')
    True
    >>> is_composite_uri('dbnd+s://localhost:8080')
    True
    >>> is_composite_uri('databand://localhost:8080')
    True
    >>> is_composite_uri('databand+s://localhost:8080')
    True
    >>> is_composite_uri('http://localhost:8080')
    False
    """
    parsed_url = urlparse(uri)
    return parsed_url.scheme in DBND2WEB_MAP


def build_composite_uri(dbnd_store_url, duplicate_tracking_to):
    """Returns dbnd store uri that contain uri to duplicate tracking data to.
    E.g. dbnd://localhost:8080?duplicate_tracking_to=http%3A%2F%2Fmlflow-store%3A80%2F

    Parameters:
        dbnd_store_url (str): dbnd store url to send tracking data to
        duplicate_tracking_to (str): mlflow store uri to duplicate tracking data to
    Returns:
        str: dbnd store composite uri to be used by MLFlow

    >>> build_composite_uri('http://localhost:8080', 'http://mlflow-store:80/')
    'dbnd://localhost:8080?duplicate_tracking_to=http%253A%252F%252Fmlflow-store%253A80%252F'
    >>> build_composite_uri('https://localhost:8080', 'http://mlflow-store:80/')
    'dbnd+s://localhost:8080?duplicate_tracking_to=http%253A%252F%252Fmlflow-store%253A80%252F'
    >>> build_composite_uri('http://localhost:8080', '')
    'dbnd://localhost:8080?duplicate_tracking_to='
    >>> build_composite_uri('http://localhost:8080', None)
    'dbnd://localhost:8080'
    """
    parsed_url = urlparse(dbnd_store_url)
    assert parsed_url.scheme in WEB2DBND_MAP

    parsed_query_dict = dict(parse_qsl(parsed_url.query))
    if duplicate_tracking_to is not None:
        parsed_query_dict["duplicate_tracking_to"] = quote_plus(duplicate_tracking_to)

    parsed_url = parsed_url._replace(scheme=WEB2DBND_MAP[parsed_url.scheme])
    parsed_url = parsed_url._replace(query=urlencode(parsed_query_dict))

    return urlunparse(parsed_url)


def parse_composite_uri(composite_uri):
    """Returns a tuple with a parsed dbnd store url and mlflow uri to duplicate tracking data to.
    E.g. dbnd://localhost:8080?duplicate_tracking_to=http%3A%2F%2Fmlflow-store%3A80%2F

    Parameters:
        composite_uri (str): dbnd store uri with dbnd schema
    Returns:
        tuple: dbnd_store_url and duplicate_tracking_to url

    >>> parse_composite_uri('dbnd://localhost:8080?duplicate_tracking_to=http%253A%252F%252Fmlflow-store%253A80%252F')
    ('http://localhost:8080', 'http://mlflow-store:80/')
    >>> parse_composite_uri('dbnd+s://localhost:8080?duplicate_tracking_to=http%253A%252F%252Fmlflow-store%253A80%252F')
    ('https://localhost:8080', 'http://mlflow-store:80/')
    >>> parse_composite_uri('dbnd://localhost:8080?duplicate_tracking_to=')
    ('http://localhost:8080', None)
    >>> parse_composite_uri('dbnd+s://localhost:8080?duplicate_tracking_to=')
    ('https://localhost:8080', None)
    >>> parse_composite_uri('databand://localhost:8080?duplicate_tracking_to=')
    ('http://localhost:8080', None)
    >>> parse_composite_uri('databand+s://localhost:8080?duplicate_tracking_to=')
    ('https://localhost:8080', None)
    >>> parse_composite_uri('dbnd://localhost:8080')
    ('http://localhost:8080', None)
    """
    parsed_url = urlparse(composite_uri)
    assert parsed_url.scheme in DBND2WEB_MAP

    parsed_query_dict = dict(parse_qsl(parsed_url.query))
    duplicate_tracking_to = parsed_query_dict.pop("duplicate_tracking_to", None)
    if duplicate_tracking_to:
        duplicate_tracking_to = unquote_plus(duplicate_tracking_to)

    parsed_url = parsed_url._replace(scheme=DBND2WEB_MAP[parsed_url.scheme])
    parsed_url = parsed_url._replace(query=urlencode(parsed_query_dict))

    return urlunparse(parsed_url), duplicate_tracking_to
