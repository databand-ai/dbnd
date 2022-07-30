# © Copyright Databand.ai, an IBM Company 2022

import tempfile

import pandas as pd

from mock import patch

from dbnd import parameter, pipeline


@pipeline
def pipeline_with_df_input(data=parameter.csv[pd.DataFrame]):
    return data


class TestPipelineParamEvaluation(object):
    def test_param_evaluation(self, pandas_data_frame):
        with tempfile.NamedTemporaryFile("w", suffix="csv", delete=False) as tmp:
            # Create file target to force read from disk
            pandas_data_frame.to_csv(tmp)
        with patch("pandas.read_csv") as mock_read_csv:
            pipeline_with_df_input.dbnd_run(tmp.name)
            # Ensure pipeline does not read parameters!
            assert mock_read_csv.call_count == 0
