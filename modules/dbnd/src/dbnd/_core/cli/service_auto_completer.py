# © Copyright Databand.ai, an IBM Company 2022

import logging
import os

from dbnd._core.cli.click_utils import _help, _help_short
from dbnd._core.cli.utils import FastSingletonContext, PrefixStore, no_errors
from dbnd._core.configuration.environ_config import is_unit_test_mode
from dbnd._core.utils.project.project_fs import databand_system_path
from dbnd._vendor.click._bashcomplete import ZSH_COMPLETION_SUFFIX


logger = logging.getLogger(__name__)

# indices
TASK_FAMILY = "task_by_family"
TASK_FULL = "task_by_full"
CONFIG_FAMILY = "config_by_family"
CONFIG_FULL = "config_by_full"
PARAM_FAMILY = "param_by_family"
PARAM_FULL = "param_by_full"

# blacklists
PARAM_BLACKLIST = ["task_env", "task_target_date"]


class AutoCompleter(FastSingletonContext):
    def __init__(self):
        self._store_ins = None

    def config(self):
        return self._build_task_completer(is_config=True)

    def task(self):
        return self._build_task_completer(is_config=False)

    def config_param(self):
        return self._build_param_completer(
            is_config=True, with_root=False, with_tasks=True
        )

    def task_param(self):
        return self._build_param_completer(
            is_config=False, with_root=True, with_tasks=True
        )

    def root_param(self):
        return self._build_param_completer(
            is_config=False, with_root=True, with_tasks=False
        )

    @no_errors()
    def refresh(self, task_classes):
        from dbnd._core.parameter.parameter_definition import _ParameterKind
        from dbnd._core.task.config import Config

        store = self._get_store(loaded=False)

        for task_cls in task_classes:
            if task_cls is Config:
                continue

            task_family = task_cls.get_task_family()
            full_task_family = task_cls.task_definition.full_task_family
            is_config = issubclass(task_cls, Config)
            description = task_cls.__doc__

            store.set(full_task_family, description)

            if is_config:
                store.index(CONFIG_FAMILY, task_family, full_task_family)
                store.index(CONFIG_FULL, full_task_family, full_task_family)
            else:
                store.index(TASK_FAMILY, task_family, full_task_family)
                store.index(TASK_FULL, full_task_family, full_task_family)

            for (
                param_name,
                param_obj,
            ) in task_cls.task_definition.task_param_defs.items():
                if (
                    param_obj.system
                    or param_obj.kind == _ParameterKind.task_output
                    or param_name in PARAM_BLACKLIST
                ):
                    continue

                param_family = task_family + "." + param_name
                param_full = full_task_family + "." + param_name

                store.set(param_full, param_obj.description)
                store.index(PARAM_FAMILY, param_family, param_full)
                store.index(PARAM_FULL, param_full, param_full)

        store.save()

    def _build_task_completer(self, is_config):
        indices = _get_indices(is_config)

        @no_errors(ret=tuple())
        def _completer(ctx, args, incomplete):
            store = self._get_store(loaded=True)
            tasks = store.search(indices, incomplete)

            if incomplete in tasks:
                return []

            res = []
            for task_name, task_desc in tasks.items():
                short_desc = _help(task_desc)
                res.append((task_name, short_desc))

            return res

        return _completer

    def _build_param_completer(self, is_config, with_root, with_tasks):
        indices = _get_indices(is_config)

        @no_errors(ret=tuple())
        def _completer(ctx, args, incomplete):
            store = self._get_store(loaded=True)
            res = [(ZSH_COMPLETION_SUFFIX, "")]

            if with_root and len(ctx.args) > 0:
                params = store.search(
                    indices_names=[PARAM_FAMILY, PARAM_FULL],
                    prefix=ctx.args[0] + "." + incomplete,
                )

                for param_name, param_desc in params.items():
                    _, param_name = param_name.rsplit(".", maxsplit=1)
                    short_desc = _help(param_desc)
                    res.append((param_name, short_desc))

            if not with_tasks:
                return res

            task_name = incomplete
            if "." in incomplete:
                task_name, _ = incomplete.rsplit(".", maxsplit=1)

            tasks = store.search(indices, task_name)

            if task_name in tasks:
                params = store.search(
                    indices_names=[PARAM_FAMILY, PARAM_FULL], prefix=incomplete
                )

                for key, param_desc in params.items():
                    short_desc = _help(param_desc)
                    res.append((key, short_desc))

                return res

            for task_name, task_desc in tasks.items():
                short_desc = _help_short(task_desc)
                res.append((task_name + ".", short_desc))

            return res

        return _completer

    def _get_store(self, loaded):
        if self._store_ins is None:

            autocompletion_cache_filename = (
                ".shell.unittest" if is_unit_test_mode() else ".shell"
            )
            store_path = databand_system_path(
                "autocompletion", autocompletion_cache_filename
            )

            self._store_ins = PrefixStore(store_path)

            if loaded and os.path.exists(store_path):
                self._store_ins.load()

        return self._store_ins


def _get_indices(is_config):
    if is_config:
        return [CONFIG_FAMILY, CONFIG_FULL]
    return [TASK_FAMILY, TASK_FULL]


completer = AutoCompleter.try_instance()
