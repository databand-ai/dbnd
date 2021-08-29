# The MIT License (MIT)
#
# Copyright (c) 2015 holger krekel (rather uses bitbucket/hpk42)
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

"""
Tracing utils
"""


class TagTracer(object):
    def __init__(self):
        self._tags2proc = {}
        self._writer = None
        self.indent = 0

    def get(self, name):
        return TagTracerSub(self, (name,))

    def _format_message(self, tags, args):
        if isinstance(args[-1], dict):
            extra = args[-1]
            args = args[:-1]
        else:
            extra = {}

        content = " ".join(map(str, args))
        indent = "  " * self.indent

        lines = ["%s%s [%s]\n" % (indent, content, ":".join(tags))]

        for name, value in extra.items():
            lines.append("%s    %s: %s\n" % (indent, name, value))

        return "".join(lines)

    def _processmessage(self, tags, args):
        if self._writer is not None and args:
            self._writer(self._format_message(tags, args))
        try:
            processor = self._tags2proc[tags]
        except KeyError:
            pass
        else:
            processor(tags, args)

    def setwriter(self, writer):
        self._writer = writer

    def setprocessor(self, tags, processor):
        if isinstance(tags, str):
            tags = tuple(tags.split(":"))
        else:
            assert isinstance(tags, tuple)
        self._tags2proc[tags] = processor


class TagTracerSub(object):
    def __init__(self, root, tags):
        self.root = root
        self.tags = tags

    def __call__(self, *args):
        self.root._processmessage(self.tags, args)

    def get(self, name):
        return self.__class__(self.root, self.tags + (name,))
