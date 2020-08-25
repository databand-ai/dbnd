import csv

import click


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
@click.option("--output", "-o", "output_file", required=True, help="path to output csv")
def generate_csv(rows, boolean_columns, str_columns, numerical_columns, output_file):
    """ Generate CSV files for histogram tests """
    half_boolean_columns = int(boolean_columns / 2)
    column_names = ["boolean_" + str(i) for i in range(boolean_columns)]
    column_names += ["str_" + str(i) for i in range(str_columns)]
    column_names += ["num_" + str(i) for i in range(numerical_columns)]

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(column_names)

        for i in range(rows):
            value_list = generate_row(
                half_boolean_columns, i, numerical_columns, str_columns
            )
            writer.writerow(value_list)
            print_progress(i, rows)

    columns = boolean_columns + str_columns + numerical_columns
    print(f"generated {rows} rows with {columns} columns")


def generate_row(half_boolean_columns, i, numerical_columns, str_columns):
    i_mod = i % 10000
    str_values = [f"str_{j}_{i_mod}" for j in range(str_columns)]
    boolean_values = [bool(i % 2)] * half_boolean_columns + [
        bool((i + 1) % 2)
    ] * half_boolean_columns
    numerical_values = [i_mod + j for j in range(numerical_columns)]
    value_list = boolean_values + str_values + numerical_values
    return value_list


def print_progress(i, rows):
    percentage = i / rows * 100
    if percentage % 10 == 0:
        percentage = int(percentage)
        print(f"{percentage}% generated {i} rows")


if __name__ == "__main__":
    generate_csv()
