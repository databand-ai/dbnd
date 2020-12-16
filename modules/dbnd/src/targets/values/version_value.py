import logging

from dbnd._core.current import get_databand_context, is_verbose
from dbnd._core.utils.basics.nothing import NOTHING, is_defined
from dbnd._core.utils.git import get_git_commit
from dbnd._core.utils.project.project_fs import project_path
from dbnd._core.utils.timezone import utcnow
from targets.values import StrValueType


logger = logging.getLogger(__name__)


class VersionAlias(object):
    now = "now"
    context_uid = "context_uid"
    git = "git"


class VersionStr(str):
    pass


class VersionValueType(StrValueType):
    """
    A Value whose value is a regular string,
    supports aliases from VersionAlias
    """

    type = VersionStr
    discoverable = False

    def parse_from_str(self, s):
        s = super(VersionValueType, self).parse_from_str(s)
        if not s:
            return s

        if s.lower() == VersionAlias.now:
            return utcnow().strftime("%Y%m%d_%H%M%S")

        if s.lower() == VersionAlias.context_uid:
            return get_databand_context().current_context_uid

        if s.lower() == VersionAlias.git:
            return str(get_project_git())

        return s


_project_git_version = NOTHING


def get_project_git():
    global _project_git_version
    if is_defined(_project_git_version):
        return _project_git_version

    _project_git_version = get_git_commit(project_path(), verbose=is_verbose())
    return _project_git_version
