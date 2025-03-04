# © Copyright Databand.ai, an IBM Company 2025
import pytest


@pytest.fixture
def mock_environment(request, monkeypatch):
    env = {"DBND__MONITOR__SYNCER_NAME": "test_syncer"}

    try:
        env.update(getattr(request.instance, "environment", {}))
    except (AttributeError, TypeError):
        pass

    with monkeypatch.context() as m:
        for key, value in env.items():
            m.setenv(key, value)
        yield
