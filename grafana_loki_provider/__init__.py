def get_provider_info():
    return {
        "package-name": "airflow-provider-grafana-loki", # Required
        "name": "Airflow Provider for Grafana Loki", # Required
        "description": "Grafana Loki Task Log Handler.",
        #"hook-class-names": ["grafana_loki_provider.hooks.loki.LokiHook"], # Depricated in >=2.2.0
        "logging":["grafana_loki_provider.log.loki_task_handler.LokiTaskHandler"],
        "connection-types": [
            {"hook-class-name": "grafana_loki_provider.hooks.loki.LokiHook", "connection-type": "grafana_loki"}
        ],
        "versions": ["0.0.1a2"] # Required
    }
