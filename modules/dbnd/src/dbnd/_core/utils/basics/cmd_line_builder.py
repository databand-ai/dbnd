class CmdLineBuilder(object):
    def __init__(self):
        self._cmd_line = []

    def add(self, *args):
        self._cmd_line.extend(args)

    def extend(self, args):
        self._cmd_line.extend(args)

    def option(self, key, value):
        if value is None:
            return
        self.add(str(key), str(value))

    def option_bool(self, key, value):
        if value:
            self.add(str(key))

    def option_dict(self, key, value):
        pass

    def get_cmd(self):
        return list(self._cmd_line)

    def get_cmd_line(self, safe_curly_brackets=False):
        return list2cmdline(self._cmd_line, safe_curly_brackets=safe_curly_brackets)


def list2cmdline(seq, safe_curly_brackets=False):
    """
    Translate a sequence of arguments into a command line
    string, using the same rules as the MS C runtime:

    1) Arguments are delimited by white space, which is either a
       space or a tab.

    2) A string surrounded by double quotation marks is
       interpreted as a single argument, regardless of white space
       contained within.  A quoted string can be embedded in an
       argument.

    3) A double quotation mark preceded by a backslash is
       interpreted as a literal double quotation mark.

    4) Backslashes are interpreted literally, unless they
       immediately precede a double quotation mark.

    5) If backslashes immediately precede a double quotation mark,
       every pair of backslashes is interpreted as a literal
       backslash.  If the number of backslashes is odd, the last
       backslash escapes the next double quotation mark as
       described in rule 3.
    """

    # Based on subprocess.list2cmd
    # See
    # http://msdn.microsoft.com/en-us/library/17w5ykft.aspx
    # or search http://msdn.microsoft.com for
    # "Parsing C++ Command-Line Arguments"
    result = []
    needquote = False
    for arg in seq:
        bs_buf = []

        # Add a space to separate this argument from the others
        if result:
            result.append(" ")

        # FIX, support brackets
        # needquote = (" " in arg) or ("\t" in arg) or not arg
        needquote = (
            (" " in arg)
            or ("\t" in arg)
            or not arg
            or (safe_curly_brackets and "{" in arg)
        )
        if needquote:
            result.append('"')

        for c in arg:
            if c == "\\":
                # Don't know if we need to double yet.
                bs_buf.append(c)
            elif c == '"':
                # Double backslashes.
                result.append("\\" * len(bs_buf) * 2)
                bs_buf = []
                result.append('\\"')
            else:
                # Normal char
                if bs_buf:
                    result.extend(bs_buf)
                    bs_buf = []
                result.append(c)

        # Add remaining backslashes, if any.
        if bs_buf:
            result.extend(bs_buf)

        if needquote:
            result.extend(bs_buf)
            result.append('"')

    return "".join(result)


def list2cmdline_safe(seq, safe_curly_brackets=True):
    return list2cmdline(seq, safe_curly_brackets=safe_curly_brackets)
