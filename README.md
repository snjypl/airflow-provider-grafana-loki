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

Writing logs to Grafana Loki
----------------------------------

Airflow can be configured to read and write task logs in Grafana Loki. It uses an existing
Airflow connection to read or write logs. If you don't have a connection properly setup,
this process will fail.

Follow the steps below to enable Grafana Loki logging:

#. Airflow's logging system requires a custom ``.py`` file to be located in the :envvar:`PYTHONPATH`, so that it's importable from Airflow. Start by creating a directory to store the config file, ``$AIRFLOW_HOME/config`` is recommended.
#. Create empty files called ``$AIRFLOW_HOME/config/log_config.py`` and ``$AIRFLOW_HOME/config/__init__.py``.
#. Copy the contents of ``airflow/config_templates/airflow_local_settings.py`` into the ``log_config.py`` file created in ``Step 2``.
#. Customize the following portions of the template:

    .. code-block:: ini

        # wasb buckets should start with "wasb" just to help Airflow select correct handler
        REMOTE_BASE_LOG_FOLDER = 'wasb://<container_name>@<storage_account>.blob.core.windows.net'

        # Rename DEFAULT_LOGGING_CONFIG to LOGGING CONFIG
        LOGGING_CONFIG = ...


#. Make sure a Grafana Loki (Loki) connection hook has been defined in Airflow. The hook should have read and write access to the Grafana Loki Api defined above in ``REMOTE_BASE_LOG_FOLDER``.

#. Update ``$AIRFLOW_HOME/airflow.cfg`` to contain:

    .. code-block:: ini

        [logging]
        remote_logging = True
        logging_config_class = log_config.LOGGING_CONFIG
        remote_log_conn_id = <name of the Grafana Loki connection>

#. Restart the Airflow webserver and scheduler, and trigger (or wait for) a new task execution.
#. Verify that logs are showing up for newly executed tasks is showing up in Airflow UI. 
