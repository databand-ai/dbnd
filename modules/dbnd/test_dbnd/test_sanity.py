# © Copyright Databand.ai, an IBM Company 2022

import logging
import os

from dbnd.testing.helpers import import_all_modules


logger = logging.getLogger(__name__)


class TestSanityTasks(object):
    def test_import_package(self):
        """Test that all module can be imported"""

        project_dir = os.path.join(os.path.dirname(__file__), "..", "src")
        good_modules = import_all_modules(
            src_dir=project_dir, package="dbnd", excluded=["airflow_operators"]
        )

        assert len(good_modules) > 20

    def test_import_dbnd(self):
        """
        Test that the top databand package can be imported and contains the usual suspects.
        """
        import dbnd

        # These should exist (if not, this will cause AttributeErrors)
        expected = [dbnd.Config, dbnd.task]
        print(expected)
