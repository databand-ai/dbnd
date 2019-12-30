_gsc = None


def _cached_gc_credentials():
    global _gsc
    if _gsc:
        return _gsc
    from dbnd_airflow_contrib.credentials_helper_gc import GSCredentials

    _gsc = GSCredentials().get_credentials()
    return _gsc


def build_gcs_client():
    from dbnd_gcp.fs.gcs import GCSClient

    return GCSClient(oauth_credentials=_cached_gc_credentials(), cache_discovery=False)
