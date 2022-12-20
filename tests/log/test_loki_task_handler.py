from grafana_loki_provider.log.loki_task_handler import LokiTaskHandler
from grafana_loki_provider.hooks.loki import LokiHook
import pytest
from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from unittest.mock import patch
from datetime import timedelta
import requests_mock
import gzip
import json
loki_log_response = {
    "data": {
        "result": [
            {
                "stream": {
                    "try_number": "3",
                    "dag_id": "test_loki_dag",
                    "map_index": "-1",
                    "run_id": "test_run_id",
                    "task_id": "test_loki_task",
                },
                "values": [
                    [
                        "1659729439359237120",
                        '{"line": "line1\\n", "run_id": "test_run_id", "try_number": 3, "map_index": -1}',
                    ],
                    [
                        "1659729439359296512",
                        '{"line": "line2\\n", "run_id": "test_run_id", "try_number": 3, "map_index": -1}',
                    ],
                    [
                        "1659729439359317504",
                        '{"line": "line3\\n", "run_id": "test_run_id", "try_number": 3, "map_index": -1}',
                    ],
                ],
            }
        ]
    }
}


expected_payload = {
    "streams": [
        {
            "stream": {"dag_id": "loki_test_dag", "task_id": "loki_test_task"},
            "values": [
                [
                    "1659996770510604032",
                    '{"line": "testline1", "run_id": "test_run_id", "try_number": 1, "map_index": 2}',
                ],
                [
                    "1659996579464455168",
                    '{"line": "testline2", "run_id": "test_run_id", "try_number": 1, "map_index": 2}',
                ],
            ],
        }
    ]
}

class TestLokiHandler:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        log_path = tmp_path / "airflow_test_log"
        log_path.mkdir()
        self.log_base_path = log_path

        self.handler = LokiTaskHandler(
            base_log_folder=str(log_path),
            name="test_handler",
            enable_gzip=True,
        )
        self.extras = dict(run_id="test_run_id", try_number=1, map_index=2)
        self.labels = dict(dag_id="loki_test_dag", task_id="loki_test_task")

        date = datetime(2016, 1, 1)
        self.dag = DAG("loki_test_dag", start_date=date)
        task = EmptyOperator(task_id="loki_test_task", dag=self.dag)
        dag_run = DagRun(
            dag_id=self.dag.dag_id,
            execution_date=date,
            run_id="test_run_id",
            run_type="manual",
        )
        with create_session() as session:
            session.query(DagRun).delete()
        with create_session() as session:
            session.add(dag_run)
            session.commit()
            session.refresh(dag_run)

        self.ti = TaskInstance(task=task, run_id=dag_run.run_id, map_index=2)
        self.ti.dag_run = dag_run
        self.ti.try_number = 1
        self.ti.start_date = date
        self.ti.end_date = date + timedelta(days=2)
        self.ti.state = State.RUNNING

        yield

        self.dag.clear()
        with create_session() as session:
            session.query(DagRun).delete()

    def test_hook_property(self, mocker):

        assert isinstance(self.handler.hook, LokiHook)

    def test_get_extras(self):
        extra = self.handler.get_extras(self.ti)
        assert extra == self.extras

    def test_get_labels(self):
        labels = self.handler.get_labels(self.ti)
        assert labels == self.labels

    def test_set_context(self, session):

        self.handler.set_context(self.ti)
        assert self.handler.labels == self.labels
        assert self.handler.extras == self.extras
        assert self.handler.upload_on_close == True

    def test_get_task_query(self):
        query = self.handler._get_task_query(self.ti, try_number=2, metadata=None)
        expected_query = """ {dag_id="loki_test_dag",task_id="loki_test_task"}
                    | json try_number="try_number",map_index="map_index",run_id="run_id"
                    | try_number="2" and
                      map_index="2" and
                      run_id="test_run_id"
                    | __error__!="JSONParserErr"
                    """
        assert str(expected_query).strip() == str(query).strip()

    def test_read(self, loki_urls):

        self.handler.hook.get_conn()
        query_range_url = loki_urls["query_range_url"]

        with requests_mock.Mocker() as m:
            m.get(query_range_url, json=loki_log_response)
            log_lines, meta = self.handler._read(self.ti, try_number=1)
            assert meta == {"end_of_log": True}
            assert log_lines == "line1\nline2\nline3\n"

    def test_close(self, mocker):
        import os

        self.handler.set_context(self.ti)
        local_loc = os.path.join(self.log_base_path, self.handler.log_relative_path)
        with open(local_loc, "w") as f:
            f.write("testLogLine1")
        self.handler.loki_write = mocker.Mock(return_value=None)
        self.handler.close()
        self.handler.loki_write.assert_called_once_with(["testLogLine1"])

    def test_build_payload(self, mocker):

        log = ["testline1", "testline2"]
        with patch(
            "time.time", side_effect=[1659996770.5106041, 1659996579.464455168]
        ):
            payload = self.handler.build_payload(
                log=log, labels=self.labels, extras=self.extras
            )
        assert payload == expected_payload

    def test_loki_write(self, mocker):

        log = ["testline1", "testline2"]
        self.handler.hook.push_log = mocker.Mock()
        self.handler.enable_gzip = False
        self.handler.set_context(self.ti)
        with patch(
            "time.time", side_effect=[1659996770.5106041, 1659996579.464455168]
        ):
            self.handler.loki_write(log)

        headers = {'Content-Type':"application/json"}
        self.handler.hook.push_log.assert_called_once_with(payload=expected_payload, headers=headers)


    def test_loki_write_with_gzip(self, mocker):
        global expected_payload
        log = ["testline1", "testline2"]
        self.handler.hook.push_log = mocker.Mock()
        self.handler.enable_gzip = True
        self.handler.set_context(self.ti)
        self.handler.build_payload = mocker.Mock(return_value=expected_payload)
        self.handler.loki_write(log)

        headers = {'Content-Type':"application/json"}
        headers['Content-Encoding']='gzip'
        payload = gzip.compress(json.dumps(expected_payload).encode("utf-8"))
        self.handler.hook.push_log.assert_called_once_with(payload=payload, headers=headers)
