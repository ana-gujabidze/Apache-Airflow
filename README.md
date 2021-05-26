# Apache-Airflow

This repository contains example DAGs that implements simple concepts from Apache Airflow and `requirements.txt` listing the necessary packages.

## Background

Airflow is a platform to programmatically author, schedule and monitor workflows. It allows you to define complex directed acyclic graphs (DAG) in a simple Python script, which represents the DAGs structure (tasks and their dependencies) as code.

Airflow has a number of built-in concepts that make data engineering simple, including DAGs (which describe how to run a workflow) and Operators (determine what actually gets done by a task). For more information view [Apache Airflow documentaion](https://airflow.apache.org/docs/apache-airflow/stable/index.html)

## Run Airflow

This project uses a `docker-compose.yaml` to consolidate common commands and make it easy for anyone to get started. To run Airflow locally, simply:
```
docker-compose up -d
```
Go to [localhost:8080](http://localhost:8080/home) to check out the UI. Login with the user-password combo: `aiflow:airflow`. This can be easily changed in `docker-compose.yaml`.

Now you can start testing your DAGs. In `docker-compose.yaml` both DAGs and plugins are mounted as volumes, because of that any changes made there will be synced to the webserver and scheduler. In this repository only code contributed in `dag`, directory that houses all Airflow DAG configuration files, is presented. 

When you are finished, you can simply stop containers by:
```
docker-compose down
```
