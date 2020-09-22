import csv

from time import time

import click
import pandas

import pyarrow

from pyarrow.parquet import ParquetWriter


_start_time = None


@click.command()
@click.option(
    "--rows", "-r", required=True, type=int, help="Number of rows to generate"
)
@click.option(
    "--booleans",
    "-b",
    "boolean_columns",
    default=0,
    help="Number of boolean columns to generate",
)
@click.option(
    "--strings",
    "-s",
    "str_columns",
    default=0,
    help="Number of string columns to generate",
)
@click.option(
    "--numericals",
    "-n",
    "numerical_columns",
    default=0,
    help="Number of numerical columns to generate",
)
@click.option(
    "--output", "-o", "output_file", required=True, help="path to output file"
)
@click.option(
    "--format", "-f", "output_format", default="csv", help="output format: csv, parquet"
)
def generate_data(
    rows, boolean_columns, str_columns, numerical_columns, output_file, output_format
):
    """ Generate files with data for histogram tests """
    global _start_time
    _start_time = time()

    column_names = ["boolean_" + str(i) for i in range(boolean_columns)]
    column_names += ["str_" + str(i) for i in range(str_columns)]
    column_names += ["num_" + str(i) for i in range(numerical_columns)]

    if output_format == "csv":
        generate_csv(
            rows,
            boolean_columns,
            str_columns,
            numerical_columns,
            output_file,
            column_names,
        )
    elif output_format == "parquet":
        generate_parquet(
            rows,
            boolean_columns,
            str_columns,
            numerical_columns,
            output_file,
            column_names,
        )
    else:
        print(f"format not supported: {output_format}")

    columns = boolean_columns + str_columns + numerical_columns
    print(f"generated {rows} rows with {columns} columns")


def generate_parquet(
    rows, boolean_columns, str_columns, numerical_columns, output_file, column_names,
):
    writer = None
    rows_per_write = int(rows / 50)

    for i in range(int(rows / rows_per_write)):
        data_rows = generate_rows(
            i * rows_per_write,
            rows_per_write,
            boolean_columns,
            numerical_columns,
            str_columns,
        )
        data = dict(zip(column_names, data_rows))
        df = pandas.DataFrame(data=data)
        table = pyarrow.Table.from_pandas(df)
        if not writer:
            writer = pyarrow.parquet.ParquetWriter(output_file, table.schema)
        writer.write_table(table=table)
        print_progress(i * rows_per_write, rows)
    writer.close()


def generate_csv(
    rows, boolean_columns, str_columns, numerical_columns, output_file, column_names,
):
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(column_names)

        for i in range(rows):
            value_list = generate_row(
                i, boolean_columns, numerical_columns, str_columns
            )
            writer.writerow(value_list)
            print_progress(i, rows)


def generate_rows(i, rows, boolean_columns, numerical_columns, str_columns):
    columns = boolean_columns + numerical_columns + str_columns
    data_rows = [[] for i in range(columns)]
    for j in range(rows):
        row = generate_row(
            i * rows + j, boolean_columns, numerical_columns, str_columns
        )
        for k, value in enumerate(row):
            data_rows[k].append(value)
    return data_rows


def generate_row(i, boolean_columns, numerical_columns, str_columns):
    i_mod = i % 10000
    str_values = [f"str_{j}_{i_mod}" for j in range(str_columns)]
    boolean_values = [bool((i + j) % 2) for j in range(boolean_columns)]
    numerical_values = [i_mod + j for j in range(numerical_columns)]
    value_list = boolean_values + str_values + numerical_values
    return value_list


def print_progress(i, rows):
    percentage = i / rows * 100
    if percentage % 10 == 0:
        percentage = int(percentage)
        duration = time() - _start_time
        print(f"{percentage}% generated {i} rows in {round(duration)} seconds")


if __name__ == "__main__":
    generate_data()
