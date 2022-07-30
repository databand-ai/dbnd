# © Copyright Databand.ai, an IBM Company 2022

import sys

from mock import MagicMock, patch

from dbnd_airflow.tracking.airflow_patching import _patch_policy, add_tracking_to_policy


def test_patch_policy_idempotent():
    def original_policy(task):
        pass

    module = MagicMock(policy=original_policy)

    # first call to _patch_policy
    _patch_policy(module)
    patched_policy = module.policy

    assert patched_policy != original_policy

    # second call to _patch_policy - shouldn't do anything
    _patch_policy(module)

    assert module.policy is patched_policy

    with patch("dbnd_airflow.tracking.airflow_patching.track_task") as track_task:
        module.policy("random_data")
        assert track_task.call_count == 1


def test_add_tracking_to_policy():
    original_policy1 = MagicMock(_dbnd_patched=None)
    original_policy2 = MagicMock(_dbnd_patched=None)

    local_settings = MagicMock(policy=original_policy1)
    dagbag = MagicMock(settings=MagicMock(policy=original_policy2))
    # Patch module imports
    with patch.dict(
        sys.modules,
        {"airflow_local_settings": local_settings, "airflow.models.dagbag": dagbag},
    ), patch("dbnd_airflow.tracking.airflow_patching.track_task") as track_task:
        add_tracking_to_policy()
        assert local_settings.policy != original_policy1
        assert dagbag.settings.policy != original_policy2

        assert original_policy1.call_count == 0
        assert track_task.call_count == 0

        local_settings.policy(1)
        assert original_policy1.call_count == 1
        assert track_task.call_count == 1

        assert original_policy2.call_count == 0
        dagbag.settings.policy(1)

        assert original_policy2.call_count == 1
        assert track_task.call_count == 2
        assert original_policy1.call_count == 1
