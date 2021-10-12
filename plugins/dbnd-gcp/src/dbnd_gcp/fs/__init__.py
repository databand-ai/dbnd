from dbnd_gcp.credentials import get_gc_credentials


def build_gcs_client():
    from dbnd_gcp.fs.gcs import GCSClient

    return GCSClient(oauth_credentials=get_gc_credentials(), cache_discovery=False)
