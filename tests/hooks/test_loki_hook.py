"""
    Test functions for Loki Hook.
"""
from urllib.parse import ParseResult
from grafana_loki_provider.hooks.loki import LokiHook
import requests_mock
from urllib.parse import urlparse, urlencode


def test_default_conn(loki_conn_details):
    hook = LokiHook()
    conn = hook.get_conn()
    assert hook.conn_type == loki_conn_details["conn_type"]
    assert hook.base_url == loki_conn_details["host"]


def test_query_range(loki_urls, loki_conn_details):
    loki_host = loki_conn_details["host"]
    hook = LokiHook()
    params = {"query": "{{dag_id==my_dag}}"}
    query_range_url = loki_urls["query_range_url"]

    with requests_mock.Mocker() as m:

        m.get(query_range_url, text="{}")
        response = hook.query_range(params=params)
        r_url = m.last_request.url
        r: ParseResult = urlparse(r_url)

        assert r.scheme + "://" + str(r.hostname) == loki_host
        assert r.query == urlencode(params)
        assert m.last_request.method == "GET"
        assert response == {}


def test_push_log(loki_urls):
    push_log_url = loki_urls["push_log_url"]
    hook = LokiHook()
    payload = {}

    import os

    print(os.environ["AIRFLOW_CONN_LOKI_DEFAULT"])
    with requests_mock.Mocker() as m:
        m.post(push_log_url)
        response = hook.push_log(payload=payload)
        r_url = m.last_request.url
        r: ParseResult = urlparse(r_url)
        assert r.scheme + "://" + str(r.hostname) + r.path == push_log_url
        assert m.last_request.method == "POST"
