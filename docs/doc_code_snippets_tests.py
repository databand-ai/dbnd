import glob
import logging
import re

from dataclasses import dataclass
from io import StringIO
from typing import List

import pytest
import steadymark

from pyflakes.api import check
from pyflakes.reporter import Reporter


@dataclass
class Snippet:
    source: str
    filename: str
    line: int

    def log_action(self, action):
        logger.info(f"\n{action} snippet in file {self.filename}:{self.line}")
        logger.info(f"\nCODE:\n{self.source}")


def get_line_number(source: str, markdown: str):
    source_lines = source.splitlines()
    file_lines = markdown.splitlines()
    for i in range(len(file_lines)):
        if source_lines == file_lines[i : i + len(source_lines)]:
            return i


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
PATTERN = r"(?:<!-- (\w+?) -->\n)?``` ?python[a-zA-Z0-9_\- ]*\n(.+?)\n```"
files = glob.glob("./sdk-docs/**/*.md", recursive=True)

snippets: List[
    Snippet
] = []  # Snippets that should be good to go, the program will only run these.
skipped: List[
    Snippet
] = (
    []
)  # Noqa - Snippets that shouldn't be run (would cause damage, require connection details...)
xfail: List[
    Snippet
] = (
    []
)  # Xfail - require parameters/variables to run, and so are unable to run on their own.


for filename in files:
    with open(filename, "r", encoding="utf-8") as file:
        markdown = file.read()
    temporary_matches = re.finditer(PATTERN, markdown, re.DOTALL)
    for match in temporary_matches:
        line = get_line_number(match.group(2), markdown)
        if not match.group(1):  # makes sure that the snippet is not tagged with noqa.
            snippets.append(
                Snippet(source=match.group(2), filename=filename, line=line)
            )
        elif match.group(1) == "noqa":
            skipped.append(Snippet(source=match.group(2), filename=filename, line=line))
        elif match.group(1) == "xfail":
            xfail.append(Snippet(source=match.group(2), filename=filename, line=line))


@pytest.mark.parametrize(
    "snippet", snippets, ids=[snippet.filename for snippet in snippets]
)
def test_snippets(snippet):
    snippet.log_action(action="testing")
    pyflakes_stream = StringIO()
    if check(snippet.source, "", Reporter(pyflakes_stream, pyflakes_stream)):
        logger.error(f"Pyflakes error: {pyflakes_stream.getvalue()}")
        pytest.fail()
    steadymark.Runner(text=f"```python\n{snippet.source}\n```").run()


@pytest.mark.parametrize("snippet", skipped)
def test_skipped(snippet):
    snippet.log_action("skipping")
    pytest.skip()


@pytest.mark.parametrize("snippet", xfail)
def test_xfails(snippet):
    snippet.log_action("xfailing")
    pytest.xfail()
