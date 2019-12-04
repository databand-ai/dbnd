from datetime import timedelta

from airflow.models import Variable


def get_template_context(execution_date):
    from airflow import macros

    ds = execution_date.strftime("%Y-%m-%d")
    ts = execution_date.isoformat()
    yesterday_ds = (execution_date - timedelta(1)).strftime("%Y-%m-%d")
    tomorrow_ds = (execution_date + timedelta(1)).strftime("%Y-%m-%d")

    ds_nodash = ds.replace("-", "")
    ts_nodash = execution_date.strftime("%Y%m%dT%H%M%S")
    ts_nodash_with_tz = ts.replace("-", "").replace(":", "")
    yesterday_ds_nodash = yesterday_ds.replace("-", "")
    tomorrow_ds_nodash = tomorrow_ds.replace("-", "")

    class VariableAccessor:
        """
        Wrapper around Variable. This way you can get variables in templates by using
        {var.value.your_variable_name}.
        """

        def __init__(self):
            self.var = None

        def __getattr__(self, item):
            self.var = Variable.get(item)
            return self.var

        def __repr__(self):
            return str(self.var)

    class VariableJsonAccessor:
        """
        Wrapper around deserialized Variables. This way you can get variables
        in templates by using {var.json.your_variable_name}.
        """

        def __init__(self):
            self.var = None

        def __getattr__(self, item):
            self.var = Variable.get(item, deserialize_json=True)
            return self.var

        def __repr__(self):
            return str(self.var)

    return {
        "ds": ds,
        "ds_nodash": ds_nodash,
        "ts": ts,
        "ts_nodash": ts_nodash,
        "ts_nodash_with_tz": ts_nodash_with_tz,
        "yesterday_ds": yesterday_ds,
        "yesterday_ds_nodash": yesterday_ds_nodash,
        "tomorrow_ds": tomorrow_ds,
        "tomorrow_ds_nodash": tomorrow_ds_nodash,
        "END_DATE": ds,
        "end_date": ds,
        "execution_date": execution_date,
        "latest_date": ds,
        "macros": macros,
        "var": {"value": VariableAccessor(), "json": VariableJsonAccessor()},
    }
