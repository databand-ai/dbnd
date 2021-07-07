import pandas as pd

from dbnd import data, output
from dbnd.tasks import PythonTask


class PandasFrameToParquet(PythonTask):
    source = data[pd.DataFrame]
    dest = output.pandas_dataframe.parquet

    def run(self):
        self.dest = self.source
