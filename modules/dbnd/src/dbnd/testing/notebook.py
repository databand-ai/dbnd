# © Copyright Databand.ai, an IBM Company 2022

import os
import tempfile


# see https://blog.thedataincubator.com/2016/06/testing-jupyter-notebooks/
def notebook_run(path):
    """Execute a notebook via nbconvert and collect output.
    :returns (parsed nb object, execution errors)
    """
    import nbformat

    from nbconvert import nbconvertapp

    # file can't be open multiple times on windows
    # https://docs.python.org/2/library/tempfile.html
    with tempfile.NamedTemporaryFile(suffix=".ipynb", delete=False) as fout:
        tmp_file_name = fout.name
    dirname, _ = os.path.split(path)

    nbconvertapp.main(
        [
            "nbconvert",
            "--to",
            "notebook",
            "--execute",
            "--ExecutePreprocessor.timeout=60",
            "--output",
            tmp_file_name,
            path,
        ]
    )

    nb = nbformat.read(tmp_file_name, nbformat.current_nbformat)

    if os.path.exists(tmp_file_name):
        os.remove(tmp_file_name)

    errors = [
        output
        for cell in nb.cells
        if "outputs" in cell
        for output in cell["outputs"]
        if output.output_type == "error"
    ]

    return nb, errors
