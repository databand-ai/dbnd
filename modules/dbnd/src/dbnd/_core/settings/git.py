# © Copyright Databand.ai, an IBM Company 2022

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

    def _raise_enforce_clean_error(self, msg):
        help_text = (
            "\nYou see this message because enforce_clean in git section is enabled."
            "\nTo temporarily disable this message use --git-allow-dirty."
        )
        raise DatabandBuildError(help_msg=msg + help_text)

    def validate_git_policy(self):
        if not self.enforce_clean:
            return

        is_dirty = is_git_dirty()

        if is_dirty is False:
            return

        if is_dirty:
            if self.allow_dirty:
                logger.warning("Runing with not commited files.")
            else:
                self._raise_enforce_clean_error("Git repo must be clean.")
        else:  # is_dirty is None
            if self.allow_dirty:
                logger.warning("Failed to get git status.")
            else:
                self._raise_enforce_clean_error("Failed to get git status.")
