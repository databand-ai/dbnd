import sys

from dbnd import project_path

# make test_dbnd available
sys.path.append(project_path("modules", "dbnd"))
