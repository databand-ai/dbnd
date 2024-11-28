# © Copyright Databand.ai, an IBM Company 2022

import os
import tempfile


def notebook_run(path):
    """Execute a notebook via nbconvert and collect output.
    :returns (parsed nb object, execution errors)
    """
    import nbformat

    from nbconvert.preprocessors import ExecutePreprocessor

    dirname, _ = os.path.split(path)
    with tempfile.NamedTemporaryFile(suffix=".ipynb", delete=False) as fout:
        tmp_file_name = fout.name

    with open(path, "r", encoding="utf-8") as f:
        notebook = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=60, kernel_name="python3")

    try:
        ep.preprocess(notebook, {"metadata": {"path": os.path.dirname(path)}})
    except Exception as e:
        print(f"An error occurred while executing the notebook: {e}")
        if os.path.exists(tmp_file_name):
            os.remove(tmp_file_name)
        return None, [e]

    with open(tmp_file_name, "w", encoding="utf-8") as f:
        nbformat.write(notebook, f)

    with open(tmp_file_name, "r", encoding="utf-8") as f:
        executed_notebook = nbformat.read(f, as_version=4)
        errors = [
            output
            for cell in executed_notebook.cells
            if "outputs" in cell
            for output in cell["outputs"]
            if output.output_type == "error"
        ]

        for cell in executed_notebook.cells:
            if "outputs" in cell:
                print(cell["outputs"])

    if os.path.exists(tmp_file_name):
        os.remove(tmp_file_name)

    return executed_notebook, errors
