

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

Installation
--------------
Install using `pip` 

`pip install airflow-provider-grafana-loki` 

Configuration Airflow to write logs to Grafana Loki
---------------------------------------------------

Airflow can be configured to read and write task logs in Grafana Loki. It uses an existing
Airflow connection to read or write logs. If you don't have a connection properly setup,
this process will fail.

Follow the steps below to enable Grafana Loki logging:

1. Airflow's logging system requires a custom ``.py`` file to be located in the :envvar:`PYTHONPATH`, so that it's importable from Airflow. Start by creating a directory to store the config file, ``$AIRFLOW_HOME/config`` is recommended.
2. Create empty files called ``$AIRFLOW_HOME/config/log_config.py`` and ``$AIRFLOW_HOME/config/__init__.py``.
3. Copy the contents of ``airflow/config_templates/airflow_local_settings.py`` into the ``log_config.py`` file created in ``Step 2``.
4. Customize the following portions of the template:

```
     elif REMOTE_BASE_LOG_FOLDER.startswith('loki'):
        LOKI_HANDLER: Dict[str, Dict[str, Union[str, bool]]] = {
            'task': {
                'class': 'grafana_loki_provider.log.loki_task_handler.LokiTaskHandler',
                'formatter': 'airflow',
                'name':"airflow_task",
                'base_log_folder': str(os.path.expanduser(BASE_LOG_FOLDER)),
                'filename_template': FILENAME_TEMPLATE
            },
        }

        DEFAULT_LOGGING_CONFIG['handlers'].update(LOKI_HANDLER)
    else:
        raise AirflowException(
            "Incorrect remote log configuration. Please check the configuration of option 'host' in "
            "section 'elasticsearch' if you are using Elasticsearch. In the other case, "
            "'remote_base_log_folder' option in the 'logging' section."
        )




```

5. Make sure a Grafana Loki (Loki) connection hook has been defined in Airflow. The hook should have read and write access to the Grafana Loki Api.

6. Update ``$AIRFLOW_HOME/airflow.cfg`` to contain:

```

        [logging]
        remote_logging = True
        remote_base_log_folder = loki
        logging_config_class= log_config.DEFAULT_LOGGING_CONFIG
        remote_log_conn_id = <name of the Grafana Loki connection>
```

7. Restart the Airflow webserver and scheduler, and trigger (or wait for) a new task execution.
8. Verify that logs are showing up for newly executed tasks is showing up in Airflow UI. 


in case you are using gevent worker class, you might face `RecursionError: maximum recursion depth exceeded` error while reading logs from Loki. 
please refer following issue for more info:[gevent/gevent/#1016]() [apache/airflow/#9118](https://github.com/apache/airflow/issues/9118)

current workaround is to add monkey patching at the top of the airflow log settings file. in this above case, ``$AIRFLOW_HOME/config/log_config.py``

eg:
```
""Airflow logging settings."""
from __future__ import annotations

import gevent.monkey
gevent.monkey.patch_all()
import os
```


----

<h3> Note: The provider is in active  development stage. All sorts of feedback, and bug reports are welcome. I will try to addresss and resolve all issues to the best of my ability </h3> 
<h3>Incase of any issue or you need any help, please feel free to open an issue. </h3>
<h3>Your contribution to the projects is highly appreciated and welcome.</h3>
