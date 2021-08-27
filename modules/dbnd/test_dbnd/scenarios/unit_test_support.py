import sys
import unittest

from dbnd.tasks.basics import dbnd_sanity_check


class TestUnitTestSupport(unittest.TestCase):
    def test_sanity(self):
        print(sys.stdout)
        dbnd_sanity_check.dbnd_run()

        print(sys.stdout)
        pass
