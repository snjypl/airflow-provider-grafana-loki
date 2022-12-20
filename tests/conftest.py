from airflow import settings
from airflow.models import Connection
import os
import pytest
from unittest import mock

@pytest.fixture(autouse=True, scope="module")
def session():
  settings.configure_orm()
  yield settings.Session

@pytest.fixture(scope='function')
def loki_conn_details():

    return  dict(conn_type ="grafana_loki",
    host="https://test.example.com",
    login="loki_username",
    password="loki_password",
    #conn_id = "loki_default"
    )

@pytest.fixture(scope='function')
def loki_urls(loki_conn_details):
    host = loki_conn_details['host']
    urls = {}
    urls['query_range_url']=host+"/loki/api/v1/query_range"
    urls['push_log_url'] = host+"/loki/api/v1/push"
    return urls



@pytest.fixture(scope='function', autouse=True)
def init_loki_connection(mocker, loki_conn_details):
    import json
    conn_details = json.dumps(loki_conn_details)
    mocker.patch.dict("os.environ",AIRFLOW_CONN_LOKI_DEFAULT=conn_details)
    mocker.patch.dict("os.environ",AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID="lOKI_DEFAULT")
