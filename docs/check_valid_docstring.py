import inspect
import logging
import re

import pydocstyle

from pydocstyle.checker import ConventionChecker

import dbnd


verbosity = 2
checked_codes = set(pydocstyle.conventions.google)
# checked_codes.difference_update({"D101", "D103", "D104", "D106"})
checked_codes.difference_update(
    {"D212", "D100", "D107", "D105", "D102"}
)  # remove "D102" to enable checking methods.
checked_codes.add("D213")  # multiline docstrings start on the second line

fields = [
    "param",
    "type",
    "return",
    "rtype",
    "keyword",
    "raise",
    "note",
    "attention",
    "bug",
    "warning",
    "version",
    "todo",
    "deprecated",
    "since",
    "status",
    "change",
    "permission",
    "requires",
    "precondition",
    "postcondition",
    "invariant",
    "summary",
    "see",
    "parameter",
    "arg",
    "argument",
    "returns",
    "returntype",
    "raises",
    "except",
    "exception",
    "kwarg",
    "kwparam",
    "variable",
    "seealso",
    "warn",
    "require",
    "requirement",
    "precond",
    "postcond",
    "org",
    "(c)",
    "changed",
]


def check_no_epydoc_rst(docstring, fields, sourcefile, objname):
    if re.search(rf"[a-zA-Z_.,?!0-9]+\n *[=-]+ *\n", docstring):
        yield f"{sourcefile}:1 in `{objname}`:\n\t\tDon't use reStructuredText-style headers, docstring must be in \
accordance with the Google Python Style Guide!"

        if re.search(r":\w+:", docstring):
            yield f"{sourcefile}:1 in `{objname}`:\n\t\tDon't use ``:<field>:``, docstring must be in accordance with\
    the Google Python Style Guide, not reStructuredText!"

    for field in fields:
        if re.search(rf"@{field}(?: \w+)?:", docstring):
            yield f"{sourcefile}:1 in `{objname}`:\n\t\tDon't use ``@{field}``, docstring must be in accordance with\
 the Google Python Style Guide, not Epydoc!"
    return None


def check_docstrings():
    conv_checker = ConventionChecker()
    for objname in dbnd.__all__:
        obj = getattr(dbnd, objname)
        docstring = obj.__doc__
        try:
            source = inspect.getsource(obj)
            sourcefile = inspect.getfile(obj)
            if docstring:
                for error in check_no_epydoc_rst(
                    docstring, fields, sourcefile, objname
                ):
                    yield error
            for error in conv_checker.check_source(source, sourcefile):
                code = getattr(error, "code", None)
                if code in checked_codes:
                    yield error
        except TypeError as err:  # Usually happens when the object is an "instance" of another object
            sourcefile = inspect.getfile(obj.__class__)
            if docstring:
                for error in check_no_epydoc_rst(
                    docstring, fields, sourcefile, objname
                ):
                    yield error
                docstring = '"""{}{}"""'.format(
                    docstring, "\n\t" if len(docstring.splitlines()) > 1 else ""
                )
                docstring = docstring.replace("\t", "    ")
            if "databand" not in sourcefile:
                continue
            for error in conv_checker.check_source(
                f"def {objname}():\n    {docstring}", sourcefile
            ):
                code = getattr(error, "code", None)
                if code in checked_codes:
                    yield error
        except OSError as err:
            if isinstance(obj.__class__, type):
                continue
            logging.warning(f"OSError! at `{objname}`\n{err}")


if __name__ == "__main__":
    num_errors = 0
    for error in check_docstrings():
        print(error)
        num_errors += 1

    if num_errors > 0:
        print(
            f"There are {num_errors} Docstring errors! For help regarding proper docstring styling, see the notion page \
at https://www.notion.so/Python-Docstring-style-guide-b78e697563634cb09c759af786ceebaa"
        )
        exit(1)
    else:
        print("There are no docstring errors!")
