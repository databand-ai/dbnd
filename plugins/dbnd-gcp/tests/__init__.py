import sys

# make test_dbnd available
from dbnd.testing.helpers import dbnd_examples_path, dbnd_module_path


sys.path.append(dbnd_module_path())
sys.path.append(dbnd_examples_path())
