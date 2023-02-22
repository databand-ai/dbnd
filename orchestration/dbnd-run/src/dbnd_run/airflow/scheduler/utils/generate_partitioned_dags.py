# © Copyright Databand.ai, an IBM Company 2022

"""
 Usage sample:

 python -m dbnd_run.airflow.scheduler.utils.generate_partitioned_dags --partitions=3 --dag-folder=dags --base-name=dbnd_dags_from_databand --template=template.py

 --dag-folder and --template can be relative or absolute path
"""
import os

from pathlib import Path
from textwrap import dedent
from typing import Optional

import click


dag_file_template = dedent(
    """# airflow will only scan files containing the text DAG or airflow. This comment performs this function

from dbnd_run.airflow.scheduler.dags_provider_from_databand import get_dags_from_databand
from dbnd_run.airflow.scheduler.utils.partitioning import partition_from_module_name

dags = get_dags_from_databand(partition=partition_from_module_name(__name__, base_filename="%base_filename%_"))
if dags:
    globals().update(dags)
"""
)


@click.command()
@click.option(
    "--partitions",
    type=click.INT,
    required=True,
    help="Total number of partitions to generate",
)
@click.option(
    "--base-name",
    default="dbnd_dags_from_databand",
    type=click.STRING,
    help="Path prefix, ex: dbnd_dags_from_databand -> dbnd_dags_from_databand_1__3.py",
)
@click.option(
    "--dag-folder",
    required=True,
    type=click.STRING,
    help="Path to AIRFLOW_HOME/dags folder",
)
@click.option("--template", type=click.STRING, help="Path custom template file")
def generate_dags_files(
    dag_folder: str, base_name: str, partitions: int, template: Optional[str]
):
    print(f"Generating {partitions} files at {dag_folder}")
    print(f"File name prefix: {base_name}")

    if not os.path.exists(dag_folder):
        print(
            f"Dag folder {dag_folder} doesn't exist. Make sure you are providing valid folder or create it before usage."
        )
        return

    existing_files = os.listdir(dag_folder)
    for dag_file in existing_files:
        if dag_file.startswith(base_name):
            raise Exception(
                f"It seems that folder already contains {base_name} files. Try to remove them first and re-run the script."
            )

    dag_folder_path = Path(dag_folder)

    files_generated = []
    dag_file_content = dag_file_template
    if template:
        with open(template, "r") as template_file:
            dag_file_content = template_file.read()

    final_content = dag_file_content.replace("%base_filename%", base_name)
    for part in range(0, partitions):
        current_path = dag_folder_path.joinpath(base_name + f"_{part}__{partitions}.py")
        files_generated.append(str(current_path))
        with open(current_path, "w") as current_file:
            current_file.write(final_content)

    print("Completed successfully.")
    print("Files generated:")
    [print(f"\t{filename}") for filename in files_generated]


if __name__ == "__main__":
    generate_dags_files()
