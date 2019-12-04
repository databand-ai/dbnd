from __future__ import absolute_import

import logging
import os
import subprocess


logger = logging.getLogger(__name__)

GIT_ENV = "DBND_PROJECT_GIT_COMMIT"


def is_git_dirty(path=None):
    if path and os.path.isfile(path):
        path = os.path.dirname(path)
    if not os.path.exists(path):
        return True
    lines = [
        line.strip()
        for line in subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=path
        ).splitlines()
        if not line.strip().startswith(b"??")
    ]

    return len(lines) > 0


def get_git_commit(path):
    env_commit = os.environ.get(GIT_ENV)
    if env_commit:
        return env_commit

    try:
        from git import Repo

        if os.path.isfile(path):
            path = os.path.dirname(path)

        repo = Repo(path, search_parent_directories=True)
        commit = repo.head.commit.hexsha
        return commit
    except Exception as ex:
        logger.error("Failed to get git version: %s", ex)
        return None
