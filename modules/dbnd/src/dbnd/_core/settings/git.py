import logging

from dbnd._core.errors import DatabandBuildError
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import config
from dbnd._core.utils.git import is_git_dirty


logger = logging.getLogger(__name__)


class GitConfig(config.Config):
    """Databand's git validator"""

    _conf__task_family = "git"

    enforce_clean = parameter(
        description="Enforce project's git to be clean. Can be overridden by allow_dirty or --git-allow-dirty"
    ).value(False)

    allow_dirty = parameter(
        description="Permit git to be dirty when enforce_clean or --git-enforce-clean is on"
    ).value(False)

    def validate_git_policy(self):
        if not self.enforce_clean:
            return

        if is_git_dirty():
            if self.allow_dirty:
                logger.warning("Runing with not commited files")
                return

            raise DatabandBuildError(
                help_msg="Git workspace must be clean."
                "\nYou see this message because enforce_clean in git section is enabled."
                "\nTo temporarily disable this message use --git-allow-dirty."
            )
