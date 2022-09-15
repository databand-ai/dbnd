# © Copyright Databand.ai, an IBM Company 2022
from http import HTTPStatus
from unittest import mock
from unittest.mock import MagicMock

from dbnd_datastage_monitor.datastage_client.datastage_api_client import (
    DataStageApiHttpClient,
)


@mock.patch("requests.session")
def test_get_runs_ids(mock_session):
    session_mock = mock_session.return_value
    mock_cams_query_response = session_mock.post.return_value
    mock_cams_query_response.json.return_value = {
        "results": [
            {"metadata": {"asset_id": "01234"}, "href": "https://myrun.com/01234"},
            {"metadata": {"asset_id": "56789"}, "href": "https://myrun.com/56789"},
        ]
    }

    client = DataStageApiHttpClient("", "")
    client.get_session = MagicMock(return_value=session_mock)
    response = client.get_runs_ids("", "")

    assert response == (
        {"01234": "https://myrun.com/01234", "56789": "https://myrun.com/56789"},
        None,
    )


@mock.patch("requests.session")
def test_get_run_info(mock_session):
    session_mock = mock_session.return_value
    mock_get_run_response = session_mock.get.return_value
    mock_get_run_response.json.return_value = {"run_mock": "data"}
    client = DataStageApiHttpClient("", "")
    client.get_session = MagicMock(return_value=session_mock)
    response = client.get_run_info("https://myrun.com/01234")

    assert response == {"run_mock": "data"}


@mock.patch("requests.session")
def test_token_refresh_get(mock_session):
    session_mock = mock_session.return_value
    mock_get_response = session_mock.get.return_value
    mock_get_response.status_code = HTTPStatus.UNAUTHORIZED
    mock_post_response = session_mock.post.return_value
    mock_post_response.json.return_value = {"access_token": "token"}
    client = DataStageApiHttpClient("", "")
    client.get_session = MagicMock(return_value=session_mock)
    client._make_get_request("")
    data = f"grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey={client.api_key}"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    # call refresh token
    session_mock.post.assert_called_with(
        client.IDENTITY_TOKEN_API_URL, data=data, headers=headers
    )
    # retry on get with new token
    session_mock.get.assert_called_with(
        url="",
        headers={
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer token",
        },
    )


@mock.patch("requests.session")
def test_token_refresh_post(mock_session):
    session_mock = mock_session.return_value
    mock_post_response = session_mock.post.return_value
    mock_post_response.status_code = HTTPStatus.UNAUTHORIZED
    mock_post_response.json.return_value = {"access_token": "token"}
    client = DataStageApiHttpClient("", "")
    client.get_session = MagicMock(return_value=session_mock)
    client._make_post_request(url="", body={"a": "b"})
    data = f"grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey={client.api_key}"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    # call refresh token
    session_mock.post.assert_any_call(
        client.IDENTITY_TOKEN_API_URL, data=data, headers=headers
    )
    # retry on post with new token
    session_mock.post.assert_called_with(
        url="",
        json={"a": "b"},
        headers={
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer token",
        },
    )


@mock.patch("requests.session")
def test_get_job(mock_session):
    session_mock = mock_session.return_value
    mock_get_job_response = session_mock.get.return_value
    mock_get_job_response.json.return_value = {"job_info": "data", "href": "link"}
    project_id = "project"
    client = DataStageApiHttpClient("", project_id=project_id)
    client.get_session = MagicMock(return_value=session_mock)
    job_id = "job"
    response = client.get_job(job_id)
    session_mock.get.assert_any_call(
        url=f"{client.DATASTAGE_CAMS_API_ASSETS_URL}/{job_id}?project_id={project_id}",
        headers={"accept": "application/json", "Content-Type": "application/json"},
    )
    session_mock.get.assert_called_with(
        url="link",
        headers={"accept": "application/json", "Content-Type": "application/json"},
    )
    assert response == {"job_info": "data", "href": "link"}


@mock.patch("requests.session")
def test_get_flow(mock_session):
    session_mock = mock_session.return_value
    mock_get_flow_response = session_mock.get.return_value
    mock_get_flow_response.json.return_value = {"flow": "data"}
    project_id = "project"
    client = DataStageApiHttpClient("", project_id=project_id)
    client.get_session = MagicMock(return_value=session_mock)
    flow_id = "flow"
    response = client.get_flow(flow_id)
    session_mock.get.assert_called_with(
        url=f"{client.FLOW_API_URL}/{flow_id}/?project_id={project_id}",
        headers={"accept": "application/json", "Content-Type": "application/json"},
    )
    assert response == {"flow": "data"}


@mock.patch("requests.session")
def test_get_run_logs(mock_session):
    session_mock = mock_session.return_value
    mock_get_logs_response = session_mock.get.return_value
    mock_get_logs_response.json.return_value = {"results": ['{"logs":"logs"}']}
    project_id = "project"
    client = DataStageApiHttpClient("", project_id=project_id)
    client.get_session = MagicMock(return_value=session_mock)
    job_id = "job"
    run_id = "run"
    response = client.get_run_logs(job_id, run_id)
    session_mock.get.assert_called_with(
        url=f"{client.DATASTAGE_JOBS_API_URL}/{job_id}/runs/{run_id}/logs?project_id={project_id}&limit=200&userfs=false",
        headers={"accept": "application/json", "Content-Type": "application/json"},
    )
    assert response == {"logs": "logs"}


@mock.patch("requests.session")
def test_get_connections_filter_senstive_data(mock_session):
    session_mock = mock_session.return_value
    mock_get_connections_response = session_mock.get.return_value
    mock_get_connections_response.json.return_value = {
        "resources": [
            {
                "metadata": {"asset_id": 123},
                "entity": {
                    "properties": {
                        "username": "user",
                        "password": "123",
                        "name": "datasource",
                    }
                },
            }
        ]
    }
    project_id = "project"
    client = DataStageApiHttpClient("", project_id=project_id)
    client.get_session = MagicMock(return_value=session_mock)
    response = client.get_connections()
    session_mock.get.assert_called_with(
        url=f"{client.DATASTAGE_CONNECTIONS_API_URL}?project_id={project_id}&limit=200&userfs=false",
        headers={"accept": "application/json", "Content-Type": "application/json"},
    )
    assert response == {
        123: {
            "entity": {"properties": {"name": "datasource"}},
            "metadata": {"asset_id": 123},
        }
    }
