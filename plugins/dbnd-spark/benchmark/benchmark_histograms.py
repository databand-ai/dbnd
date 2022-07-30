# © Copyright Databand.ai, an IBM Company 2022

from .utils import generate_reports


def test_histograms_and_columns_variations():
    exp_name = "histograms_and_columns_variations"
    exp_params = {
        "input_file": [
            "s3://dbnd-dev-playground/data/benchmark_10_columns_1M_rows.csv"
        ],
        "output_file": ["out"],
        "to_pandas": [False],
        "with_histograms": [True],
        "sampling_type": [None],
        "sampling_fraction": [None],
        "columns_number_multiplicator": [1.5, 2, 3, 5],
    }

    generate_reports(exp_name, exp_params)


def test_histograms_default():
    exp_name = "histograms_default"
    exp_params = {
        "input_file": [
            "s3://dbnd-dev-playground/data/benchmark_10_columns_1M_rows.csv",
            "s3://dbnd-dev-playground/data/benchmark_10_columns_10M_rows.csv",
        ],
        "output_file": ["out"],
        "to_pandas": [False],
        "with_histograms": [False, True],
        "sampling_type": [None],
        "sampling_fraction": [None],
        "columns_number_multiplicator": [1],
    }

    generate_reports(exp_name, exp_params)


def test_histograms_and_sampling_variations():
    exp_name = "histograms_and_sampling_variations"
    exp_params = {
        "input_file": [
            "s3://dbnd-dev-playground/data/benchmark_10_columns_1M_rows.csv",
            "s3://dbnd-dev-playground/data/benchmark_10_columns_10M_rows.csv",
        ],
        "output_file": ["out"],
        "to_pandas": [False],
        "with_histograms": [True],
        "sampling_type": ["random", "first"],
        "sampling_fraction": [0.01, 0.05, 0.1, 0.2, 0.5],
        "columns_number_multiplicator": [1],
    }

    generate_reports(exp_name, exp_params)
