# © Copyright Databand.ai, an IBM Company 2022

from enum import Enum


class Adapter(Enum):
    # This class represents supported adapters.
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    SPARK = "spark"
    OTHER = "other"

    @staticmethod
    def adapters() -> str:
        # String representation of all supported adapter names
        return ",".join([f"`{x.value}`" for x in list(Adapter)])

    @classmethod
    def from_profile(cls, profile):
        try:
            return Adapter[profile["type"].upper()]
        except KeyError:
            return Adapter.OTHER

    def extract_host(self, profile):
        if self == Adapter.SNOWFLAKE:
            return {"account": profile.get("account")}

        if self == Adapter.BIGQUERY:
            return {
                "project": profile.get("project"),
                "location": profile.get("location"),
            }

        if self == Adapter.REDSHIFT:
            host = profile["host"]
            return {
                "hostname": host,
                "host": host,
            }  # BE expect `hostname` but original is `host`

        if self == Adapter.SPARK:
            host = profile["host"]
            return {
                "hostname": host,
                "host": host,
            }  # BE expect `hostname` but original is `host`

        host = profile.get("host", "")
        return {"hostname": host, "host": host}

    def connection(self, profile):
        conn_type = profile["type"] if self == Adapter.OTHER else self.value
        conn = {"type": conn_type}
        conn.update(self.extract_host(profile))
        return conn
