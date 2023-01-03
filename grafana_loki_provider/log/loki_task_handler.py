"""Loki logging handler for tasks"""
import time
from typing import Any, Dict, Optional, Tuple, List
import typing
import gzip

if typing.TYPE_CHECKING:
    from airflow.models import TaskInstance
from typing import Optional, Tuple
import logging

logging.raiseExceptions = True
import time
from typing import Optional
import json
from datetime import timedelta
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin
from grafana_loki_provider.hooks.loki import LokiHook
from airflow.compat.functools import cached_property
from airflow.configuration import conf
import os

BasicAuth = Optional[Tuple[str, str]]

DEFAULT_LOGGER_NAME = "airflow"

import json

import logging
import typing


class LokiTaskHandler(FileTaskHandler, LoggingMixin):
    def __init__(
        self,
        base_log_folder,
        name,
        filename_template: Optional[str] = None,
        enable_gzip=True,
    ):
        super().__init__(base_log_folder, filename_template)
        self.name: str = name
        self.handler: Optional[logging.FileHandler] = None
        self.log_relative_path = ""
        self.closed = False
        self.upload_on_close = True
        self.enable_gzip = enable_gzip
        self.labels: Dict[str, str] = {}
        self.extras: Dict[str, Any] = {}

    @cached_property
    def hook(self) -> LokiHook:
        """Returns LokiHook"""

        remote_conn_id = str(conf.get("logging", "REMOTE_LOG_CONN_ID"))

        from grafana_loki_provider.hooks.loki import LokiHook

        return LokiHook(loki_conn_id=remote_conn_id)

    def get_extras(self, ti, try_number=None) -> Dict[str, Any]:

        return dict(
            run_id=getattr(ti, "run_id", ""),
            try_number=try_number if try_number != None else ti.try_number,
            map_index=getattr(ti, "map_index", ""),
        )

    def get_labels(self, ti) -> Dict[str, str]:

        return {"dag_id": ti.dag_id, "task_id": ti.task_id}

    def set_context(self, task_instance: "TaskInstance") -> None:

        super().set_context(task_instance)

        ti = task_instance

        self.log_relative_path = self._render_filename(ti, ti.try_number)
        self.upload_on_close = not ti.raw

        # Clear the file first so that duplicate data is not uploaded
        # when re-using the same path (e.g. with rescheduled sensors)
        if self.upload_on_close:
            if self.handler:
                with open(self.handler.baseFilename, "w"):
                    pass
        self.labels = self.get_labels(ti)
        self.extras = self.get_extras(ti)

    def _get_task_query(self, ti, try_number, metadata) -> str:
        run_id = getattr(ti, "run_id", "")
        map_index = getattr(ti, "map_index", "")

        query = """ {{dag_id="{dag_id}",task_id="{task_id}"}}
                    | json try_number="try_number",map_index="map_index",run_id="run_id"
                    | try_number="{try_number}" and
                      map_index="{map_index}" and
                      run_id="{run_id}"
                    | __error__!="JSONParserErr"
                """.format(
            try_number=try_number,
            map_index=map_index,
            run_id=run_id,
            dag_id=ti.dag_id,
            task_id=ti.task_id,
        )

        return query

    def _read(
        self, ti, try_number: int, metadata: Optional[str] = None
    ) -> Tuple[str, Dict[str, bool]]:

        query = self._get_task_query(ti, try_number, metadata)

        start = ti.start_date - timedelta(days=15)
        end = ti.end_date + timedelta(hours=1)

        params = {
            "query": query,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "direction": "forward",
        }

        self.log.info(f"loki log query params {params}")
        data = self.hook.query_range(params)

        lines = []

        if "data" in data and "result" in data["data"]:
            for i in data["data"]["result"]:
                for v in i["values"]:
                    try:
                        msg = v[1]
                        line = json.loads(msg)["line"]
                        lines.append(line)
                    except Exception as e:
                        self.log.exception(e)
                        pass

        log_lines = "".join(lines)

        return log_lines, {"end_of_log": True}

    def close(self):
        """Close and upload local log file to remote storage Loki."""

        if self.closed:
            return

        super().close()

        if not self.upload_on_close:
            return

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc) as logfile:
                log = logfile.readlines()
            self.loki_write(log)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def build_payload(self, log: List[str], labels, extras) -> dict:
        """Build JSON payload with a log entry."""
        ns = 1e9
        lines = []
        for line in log:
            ts = str(int(time.time() * ns))
            line = {**{"line": line}, ** extras }
            line = json.dumps(line)
            lines.append([ts, line])

        stream = {
            "stream": labels,
            "values": lines,
        }
        return {"streams": [stream]}

    def loki_write(self, log):
        payload = self.build_payload(log, self.labels, self.extras)

        headers = {"Content-Type": "application/json"}
        if self.enable_gzip:
            payload = gzip.compress(json.dumps(payload).encode("utf-8"))
            headers["Content-Encoding"] = "gzip"

        self.hook.push_log(payload=payload, headers=headers)
