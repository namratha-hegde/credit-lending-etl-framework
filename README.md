# Credit Lending – Config‑Driven ETL \& Data Quality Platform

## Repository \& Documentation

This repository delivers a **config‑driven, production‑ready ETL and Data Quality framework** designed for a regulated financial environment. The solution demonstrates senior‑level data engineering practices, with a strong emphasis on **governance, extensibility, and auditability**.

\---

## 1\. Overview

The platform processes credit‑lending data using a **Raw → Silver → Gold** architecture and applies a reusable **Data Quality (DQ) framework** before promoting data to analytical outputs.

Key characteristics:

* Clear separation between **framework** and **pipelines**
* Fully **configuration‑driven behavior** (no hardcoded logic)
* Deterministic and **re‑runnable execution**
* Built‑in **control and audit metadata**
* Mandatory **templating** (Airflow DAGs and SQL DQ checks)
* Mandatory **unit tests** using pytest

Cloud deployment is not required; the solution runs locally while remaining **Azure‑aligned** (ADLS Gen2, Databricks, Airflow).

\---

## 2\. Repository Structure

```
Credit\_Lending/
├── airflow/                  # Airflow DAG templating (Jinja2)
│   ├── templates/
│   ├── rendered/
│   └── generator.py
│
├── config/                   # All configuration (no hardcoding)
│   ├── datasets.yml
│   ├── pipelines.yml
│   ├── dq\_rules.yml
│   ├── dq\_thresholds.yml
│   └── environments.yml
│
├── framework/                # Reusable framework components
│   ├── control/
│   │   └── control\_record.py
│   ├── dq/
│   │   ├── executor.py
│   │   ├── rules.py
│   │   ├── results.py
│   │   ├── filter.py
│   │   └── sql\_renderer.py
│   └── etl/
│       ├── reader.py
│       ├── transformer.py
│       └── pipeline\_registry.py
│
├── ingestion/                # Raw / Silver / Gold execution logic
│   ├── raw.py
│   ├── silver.py
│   └── gold.py
│
├── templates/                # SQL validation templates (Jinja2)
├── test/                     # Unit tests (pytest)
│   ├── test\_config\_loader.py
│   ├── test\_transformations.py
│   └── test\_dq\_rules.py
│
├── utils/
│   ├── \_\_init\_\_.py
│   └── config\_loader.py
│
├── main.py                   # Generic orchestration entrypoint
├── requirements.txt
└── README.md
```

\---

## 3\. How to Run

### 3.1 Prerequisites

* Python 3.10+
* Local Java installation (for Spark)

### 3.2 Install Dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3.3 Run Unit Tests (Mandatory)

```bash
pytest test/
```

Unit tests cover:

* Configuration parsing and validation
* At least one Spark transformation
* At least one Data Quality rule

> \*\*Note:\*\* Missing tests results in assignment failure.

### 3.4 Run the ETL Pipeline

```bash
python main.py
```

The pipeline runs in the **dev** environment by default. Environment‑specific behavior is controlled via `config/environments.yml`.

\---

## 4\. Data Quality Framework

The reusable DQ framework supports:

* **Completeness**
* **Uniqueness**
* **Validity**

Features:

* Rules defined via YAML configuration
* Generic execution engine
* Structured, auditable results
* Supports **FAIL / WARN / CONTINUE** semantics

DQ checks are executed at the **Silver layer**, ensuring that only validated data is promoted to Gold.

\---

## 5\. Control \& Audit

The platform captures control metadata for every execution:

* Input and output row counts
* Processing timestamps
* Dataset and layer identifiers
* Run‑level traceability via `run\_id`

This enables:

* End‑to‑end lineage
* Replay and reprocessing
* Reconciliation and auditability

\---

## 6\. Templating (Mandatory)

### 6.1 Airflow DAG Generation

* DAGs are generated using **Jinja2 templates**
* Templates define structure only (no business logic)
* Rendered DAGs are provided for inspection

### 6.2 SQL‑Based DQ Validations

* SQL validation patterns are templated with Jinja2
* Both templates and rendered SQL outputs are included

This avoids duplication while keeping business rules configuration‑driven.

\---

## 7\. Design Decisions

* **Config‑driven approach** to minimize code changes
* **Medallion architecture** (Raw/Silver/Gold) for clarity and governance
* **DQ and control logic centralized** for consistent enforcement
* **Lightweight plug‑in model** for pipeline extensibility

\---

## 8\. Trade‑offs \& Assumptions

### Trade‑offs

* Local execution is used instead of full cloud deployment
* Spark is used for transformations; Polars is used for DQ result processing
* Control metadata is implemented minimally to avoid over‑engineering

### Assumptions

* Input data is fictive and well‑structured
* Cloud security (IAM, secrets) is handled outside this scope
* Airflow and Databricks are assumed as orchestration/runtime platforms

\---

**Limitations:**

* Transformations are partially implemented in code
* Future improvement: fully config-driven transformation layer



**Design Trade-off:**

* Prioritized clarity and modularity over full abstraction within time constraints



## 9\. Final Notes

This solution prioritizes **clarity, governance, and extensibility** over complexity. All design choices are intentional, documented, and defensible in a senior‑level technical review.

