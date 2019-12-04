import sys

from dbnd._core.utils.project.project_fs import project_path

# make test_dbnd available
sys.path.append(project_path("modules", "dbnd"))
