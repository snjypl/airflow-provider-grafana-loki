from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

"""Perform the package airflow-provider-grafana-loki setup."""
setup(
    name="airflow-provider-grafana-loki",
    version="0.0.1",
    description="A provider package for pushing and reading airflow task log from Grafana Loki.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=grafana_loki_provider.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=[
        "grafana_loki_provider",
        "grafana_loki_provider.hooks",
        "grafana_loki_provider.log",
    ],
    install_requires=["apache-airflow>=2.0"],
    extras_require={"test": ["pytest>=7.0.0", "requests-mock", "pytest-mock"]},
    setup_requires=["setuptools", "wheel"],
    author="Snjypl",
    author_email="toepoe.py@gmail.com",
    url="http://grafana.com/",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires="~=3.7",
)
