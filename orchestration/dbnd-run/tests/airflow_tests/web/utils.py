# © Copyright Databand.ai, an IBM Company 2022

import functools
import json
import logging

from typing import Any

import pytest

from airflow.settings import Session
from flask import url_for
from pytest import fixture
from six.moves.urllib.parse import quote_plus

from dbnd_run.airflow.compat import AIRFLOW_VERSION_AFTER_2_2
from targets import target


logger = logging.getLogger(__name__)
skip_py2_non_compatible = functools.partial(
    pytest.mark.skip, "Python 2 non compatible code"
)


def _assert_in_resp_html(text, resp_html):
    if text not in resp_html:
        target("/tmp/ttt.html").write(resp_html)
        logger.info(resp_html)
        logger.error("Can't find %s", text)
    assert text in resp_html


def assert_content_in_response(text, resp, resp_code=200):
    resp_html = resp.data.decode("utf-8")
    assert resp_code == resp.status_code, "Response: %s" % str(resp)
    if not isinstance(text, list):
        text = [text]
    for kw in text:
        _assert_in_resp_html(kw, resp_html)
    return resp_html


def assert_api_response(resp, resp_code=200, endpoint=None, verbose=True):
    assert (
        resp_code == resp.status_code
    ), "Endpoint: %s\nResponse code: %s\nResponse data: %s" % (
        str(endpoint),
        str(resp.status_code),
        str(resp.data),
    )
    resp_data = json.loads(resp.data.decode())
    if verbose:
        logger.info("Response: %s", resp_data)
    return resp_data


def assert_ok(resp):
    assert resp.status_code == 200


def percent_encode(obj):
    return quote_plus(str(obj))


class WebAppCtrl(object):
    def __init__(self, app, appbuilder, client):
        self.app = app
        self.appbuilder = appbuilder
        self.client = client

        self.session = Session()

    def login(self):
        if AIRFLOW_VERSION_AFTER_2_2:
            from airflow.www.fab_security.sqla.models import User as ab_user
        else:
            from flask_appbuilder.security.sqla.models import User as ab_user

        sm_session = self.appbuilder.sm.get_session()
        self.user = sm_session.query(ab_user).filter(ab_user.username == "test").first()
        if not self.user:
            role_admin = self.appbuilder.sm.find_role("Admin")
            self.appbuilder.sm.add_user(
                username="test",
                first_name="test",
                last_name="test",
                email="test@fab.org",
                role=role_admin,
                password="test",
            )
            self.user = (
                sm_session.query(ab_user).filter(ab_user.username == "test").first()
            )
        return self.client.post(
            "/login/",
            data=dict(username="test", password="test"),
            follow_redirects=True,
        )

    def logout(self):
        return self.client.get("/logout/")

    def clear_table(self, model):
        self.session.query(model).delete()
        self.session.commit()
        self.session.close()


class WebAppTest(object):
    @fixture(autouse=True)
    def _set_values(self, web_app_ctrl):
        self.web = web_app_ctrl  # type: WebAppCtrl

    @property
    def app(self):
        return self.web.app

    @property
    def client(self):
        return self.web.client

    @property
    def session(self):
        return self.web.session

    def _url(self, endpoint, **kwargs):
        if endpoint.startswith("/"):
            from urllib.parse import urlencode

            url = endpoint
            if kwargs:
                url += "?" + urlencode(kwargs)
        else:
            url = url_for(endpoint, **kwargs)
        logger.info("URL = %s", url)
        return url

    def _get(self, endpoint, **kwargs):
        follow_redirects = kwargs.pop("follow_redirects", True)
        url = self._url(endpoint, **kwargs)
        return self.client.get(url, follow_redirects=follow_redirects)

    def _post(self, endpoint, **kwargs):
        follow_redirects = kwargs.pop("follow_redirects", True)
        url = self._url(endpoint, **kwargs)
        return self.client.post(url, follow_redirects=follow_redirects)

    def assert_view(self, endpoint, expected, **kwargs):
        resp = self._get(endpoint, **kwargs)
        return assert_content_in_response(expected, resp)

    def assert_api(self, endpoint, **kwargs):
        resp = self._get(endpoint, **kwargs)
        return assert_api_response(resp, endpoint=endpoint)

    @fixture(autouse=True)
    def _with_login(self, web_app_ctrl):  # type: (WebAppCtrl) -> Any
        yield web_app_ctrl.login()
        web_app_ctrl.logout()
