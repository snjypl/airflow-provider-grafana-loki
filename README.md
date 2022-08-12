<p align="center">
  <a href="https://www.airflow.apache.org">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
  </a>
</p>
<h1 align="center">
  Airflow Grafana Loki Provider
</h1>
  <h3 align="center">
  Log Handler for pushing Airflow Task Log to Grafana Loki
</h3>

<br/>

This package provides Hook and LogHandler that integrates with Grafana Loki. LokiTaskLogHandler is a python log handler that handles and reads task instance logs. It extends airflow FileTaskHandler and uploads to and reads from Grafana Loki.

