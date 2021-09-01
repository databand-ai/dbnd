# MIT LICENCE
# from https://github.com/skorokithakis/tbvaccine/commit/82529b2b91d38d20f1814cfa6701b2169b888797

# BASED ON https://github.com/skorokithakis/tbvaccine/
# Original code is licenced with

# The MIT License (MIT)
#
# Copyright (c) 2016 Stavros Korokithakis
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import re
import sys
import traceback

from pygments import highlight
from pygments.formatters.terminal256 import Terminal256Formatter as TerminalFormatter
from pygments.lexers.python import PythonLexer

#  term colour control codes
from dbnd._core.errors.errors_utils import UserCodeDetector


re_ansi_control_codes = re.compile(r"\x1b[^m]*m")


class State:
    no_idea = 0
    in_traceback = 1


class TBVaccine:
    def __init__(
        self,
        user_code_detector=None,
        isolate=True,
        show_vars=False,
        max_length=120,
        skip_non_user_on_isolate=True,
        no_colors=False,
        color_scheme="monokai",
    ):
        self._user_code_detector = (
            user_code_detector or UserCodeDetector.build_code_detector()
        )

        self.skip_non_user_on_isolate = skip_non_user_on_isolate and isolate
        # Whether to print interesting lines in color or not. If False,
        # all lines are printed in color.
        self._isolate = isolate
        self._no_colors = no_colors

        # Whether to print variables for stack frames.
        self._show_vars = show_vars and isolate

        # Max length of printed variable lines
        self._max_length = max_length

        self.color_scheme = color_scheme

        try:
            self.pygments_lexer = PythonLexer()
            self.pygments_formatter = TerminalFormatter(style=self.color_scheme)
        except Exception:
            self.pygments_lexer = None
            self.pygments_formatter = None

    def _new_tb_print(self):
        return _TBMessage(self)

    def _highlight_line(self, line):
        if self._no_colors or not self.pygments_lexer:
            return line.rstrip("\r\n")

        line = highlight(line, self.pygments_lexer, self.pygments_formatter)
        return line.rstrip("\r\n")

    def _highlight_text(self, text, fg=None, style=None, max_length=None):
        if self._no_colors:
            return text

        raw_text = re.sub(re_ansi_control_codes, "", text)
        if max_length and len(raw_text) > max_length:
            short_text = text[: int(max_length * 3)]
            # Check if there's an ANSI escape in the last few chars of max_length and break before it.
            if "\x1b" in short_text[-10:]:
                short_text = short_text[: short_text.rfind("\x1b")]
            text = short_text + "\x1b[0m ... ({} more chars)".format(
                len(text) - len(short_text)
            )
        if fg or style:
            styles = {"bright": 1, None: 0}
            colors = {
                "black": 30,
                "red": 31,
                "green": 32,
                "yellow": 33,
                "blue": 34,
                "magenta": 35,
                "cyan": 36,
                "gray": 37,
            }
            text = "\x1b[%d;%dm%s\x1b[m" % (styles[style], colors[fg], text)
        return text

    def format_tb(self, etype, value, tb):
        """
        Format an entire traceback string with ANSI colors.
        """
        return self._new_tb_print().format_tb(etype, value, tb)

    def format_exc(self):
        """
        Format the latest exception's traceback.
        """
        return self.format_tb(sys.exc_info()[2])

    def print_exc(self):
        """
        Format the latest exception's traceback.
        """
        msg = self.format_exc()
        sys.stderr.write(msg)


class _TBMessage(object):
    TB_END_RE = re.compile(r"^(?P<exception>[\w\.]+)\: (?P<description>.*?)$")
    TB_FILE_RE = re.compile(
        r'^  File "(?P<filename>.*?)", line (?P<line>\d+), in (?P<func>.*)$'
    )
    VAR_PREFIX = "|     "

    def __init__(self, tbv):
        # type: (_TBMessage, TBVaccine) -> None
        self.tbv = tbv
        # Our current state.
        self._state = State.no_idea

        # The filename of the line we're currently printing.
        self._file = None

        # The buffer that we use to build up the output in.
        self._buffer = ""

        self._found_user_code = False
        self._current_filtered_lines = []

    def _user_space(self):
        """
        Decide whether the file in the traceback is one in our code_dir or not.
        """
        if not self.tbv._user_code_detector:
            return True
        return self.tbv._user_code_detector.is_user_file(self._file)

    def _print(self, text, fg=None, style=None, max_length=None):

        self._buffer += self.tbv._highlight_text(
            text, fg=fg, style=style, max_length=max_length
        )

    def _process_var_line(self, line):
        """
        Process a line of variables in the traceback.
        """
        if not self.tbv._show_vars:
            # Don't print.
            return False

        if not self._user_space():
            return False

        line = self.tbv._highlight_line(line)
        self._print(line, max_length=self.tbv._max_length)
        return True

    def _process_code_line(self, line):
        """
        Process a line of code in the traceback.
        """
        if not self._user_space():
            if self.tbv.skip_non_user_on_isolate and not self._found_user_code:
                return False
            # Print without colors.
            self._print(line)
        else:
            if self.tbv._isolate:
                line = line[1:]
                self._print(">", fg="red", style="bright")
            self._print(line, fg="gray")

        return True

    def _process_file_line(self, line):
        """
        Process a "file" line of traceback.
        """
        match = self.TB_FILE_RE.match(line).groupdict()
        self._file = match["filename"]

        if not self._user_space():
            if self.tbv.skip_non_user_on_isolate and not self._found_user_code:
                return False
            # Print without colors.
            self._print(line)
        else:
            # let mark that we have seen some user code
            self._found_user_code = True
            self._print('  File "')
            base, fn = os.path.split(match["filename"])
            if base:
                self._print(base + os.sep, "cyan")
            self._print(fn, "cyan", style="bright")
            self._print('", line ')
            self._print(match["line"], "yellow")
            self._print(", in ")
            self._print(match["func"], "magenta")
        return True

    def _process(self, lines):
        """
        Process a line of input.
        """
        for line in lines:
            sl = line.rstrip("\r\n")
            if not sl:
                continue

            printed = True
            if self._state == State.no_idea:
                if sl == "Traceback (most recent call last):":
                    self._found_user_code = False
                    self._current_filtered_lines = []
                    # The first line of the traceback.
                    self._state = State.in_traceback
                    self._print(sl, "blue")
                else:
                    self._print(sl)
            elif self._state == State.in_traceback:
                if self.TB_END_RE.match(sl):
                    # The last line of the traceback.
                    if not self._found_user_code and self._current_filtered_lines:
                        self._print("\n".join(self._current_filtered_lines))
                    self._state = State.no_idea
                    matches = self.TB_END_RE.match(sl).groupdict()
                    self._print(matches["exception"], "red", "bright")
                    self._print(": ")
                    self._print(matches["description"], "green")
                    self._print("\n")
                elif self.TB_FILE_RE.match(line):
                    # A file line.
                    printed = self._process_file_line(sl)
                elif sl.startswith(self.VAR_PREFIX):
                    printed = self._process_var_line(sl)
                elif sl.startswith("    "):
                    # A code line.
                    printed = self._process_code_line(sl)
                else:
                    self._print(sl)

            if printed:
                self._print("\n")
            else:
                self._current_filtered_lines.append(line)

    # def format_tb_with_locals(self, etype, value, tb):
    #     """
    #     Return a traceback as a string, with the local variables in each stack.
    #     """
    #     original_tb = tb
    #     while True:
    #         if not tb.tb_next:
    #             break
    #         tb = tb.tb_next
    #
    #     stack = []
    #     f = tb.tb_frame
    #     while f:
    #         stack.append(f)
    #         f = f.f_back
    #     stack.reverse()
    #
    #     lines = ["Traceback (most recent call last):\n"]
    #     for frame, line in zip(stack, traceback.format_tb(original_tb)):
    #         # Frame lines contain newlines, so we need to split on them.
    #         lines.extend(line.split("\n"))
    #         var_tuples = sorted(frame.f_locals.items())
    #         if not var_tuples:
    #             # There are no locals, so continue.
    #             continue
    #
    #         max_length = max([len(x[0]) for x in var_tuples])
    #         for key, val in var_tuples:
    #             if type(val) in (type(lambda: None), type(sys)) or (
    #                 key.startswith("__") and key.endswith("__")
    #             ):
    #                 # We don't want to print functions or modules or __variables__.
    #                 continue
    #             try:
    #                 val = str(val)
    #             except:  # noqa
    #                 val = "<CANNOT CONVERT VALUE>"
    #             lines.append(
    #                 "%s%s = %s" % (self.VAR_PREFIX, key.ljust(max_length), val)
    #             )
    #     lines.append("%s: %s" % (value.__class__.__name__, value))
    #
    #     for line in lines:
    #         self._process_line(line)
    #
    #     return self._buffer

    def format_tb(self, etype, value, tb):
        """
        Format an entire traceback string with ANSI colors.
        """

        tb_string = traceback.format_exception(etype, value, tb)
        tb_string = "".join(tb_string)
        self._process(tb_string.split("\n"))
        return self._buffer


def add_hook(*args, **kwargs):
    if not getattr(sys.stderr, "isatty", lambda: False)():
        return
    tbv = TBVaccine(*args, **kwargs)
    sys.excepthook = tbv.print_exc
