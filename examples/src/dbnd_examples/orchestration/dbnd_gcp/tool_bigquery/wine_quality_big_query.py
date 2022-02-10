from dbnd_gcp import BigQuery


class FetchDataFromBQ(BigQuery):
    """Fetch wine quality data from Google's BigQuery"""

    query = "SELECT * FROM `wine_ds.wine_table`"
