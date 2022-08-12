import requests
from typing import Dict

from airflow.providers.http.hooks.http import HttpHook

class LokiHook(HttpHook):
    """
    Loki Hook that interacts with an log push and query endpoint.

    :param loki_conn_id: connection that has the base API url i.e https://www.grafana.com/
        and optional authentication credentials. Default headers can also be specified in
        the Extra field in json format.
    :type loki_conn_id: str
    """

    conn_name_attr  = 'loki_conn_id'
    default_conn_name = 'loki_default'
    conn_type = 'grafana_loki'
    hook_name = 'Grafana Loki'

    v1_base_endpoint = "/loki/api/v1/{method}"

    def __init__(self, loki_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(http_conn_id = loki_conn_id, *args, **kwargs)
        self.loki_conn_id =loki_conn_id



    def query_range(self, params, headers=None) -> Dict:
        query_range_endpoint = self.v1_base_endpoint.format(method="query_range")
        self.method = "GET"
        response = self.run(query_range_endpoint, data=params,headers=headers)
        response.raise_for_status()
        return response.json()


    def push_log(self, payload, headers=None) -> requests.Response:
        push_endpoint = self.v1_base_endpoint.format(method="push")
        self.method = "POST"
        response = self.run(push_endpoint, data=payload, extra_options={"timeout":3}, headers=headers)
        response.raise_for_status()
        return response

