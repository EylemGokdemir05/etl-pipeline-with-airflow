# ETL Pipeline with Airflow & dbt

A production‑oriented EL(T) pipeline orchestrated with **Apache Airflow** and **dbt (Data Build Tool)** using **Docker** & **Docker‑in‑Docker (dind)**.

This repository showcases how to operationalize dbt data transformations inside Airflow DAGs by leveraging containerization and clean environment isolation.

---

## 🚀 Project Overview

This project defines a modern EL(T) workflow using:

- **Airflow** to schedule and orchestrate jobs,
- **DockerOperator** to run dbt inside containers,
- **dbt** to build, test, and document analytics models,
- **Docker‑in‑Docker (dind)** to enable Airflow containers to execute other containerized tasks safely.

The goal is to produce tested and documented models in an automated manner while keeping your repository clean and reproducible.

---

## 🧱 Architecture

```text
Airflow Scheduler
      ↓
DockerOperator runs dbt deps → dbt build → dbt docs
      ↓
Generated docs copied to shared volume
```


## 🛠 Workflow Steps
### 1️⃣ Install Dependencies

```text

pip install -r requirements.txt

```
or bring up Airflow via Docker:
```text

docker-compose up -d

```
### 2️⃣ Access Airflow UI
Navigate to:
```text

http://localhost:8080

```
Trigger the DAG named:
```text

dbt_build_with_dockeroperator

```
