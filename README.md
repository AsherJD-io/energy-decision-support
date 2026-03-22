# Energy Decision Support System (DSS)

*A reproducible data engineering pipeline for electricity demand analytics and operational insight.*

---

## Overview

This project implements a **data engineering pipeline for electricity demand analytics** using publicly available grid data from the **ENTSO-E Transparency Platform**.

The system ingests hourly electricity demand observations, validates the integrity of the time series, and produces analytical views that support **energy system monitoring and decision support**.

The pipeline emphasizes:

- reproducible data engineering workflows
- explicit data quality validation
- transparent analytical transformations

---

## System Architecture

The system implements a layered analytical warehouse model:

```
Raw Layer
   ↓
Quality Validation Layer
   ↓
Clean Analytical Layer
   ↓
Decision Support Views
```

A detailed explanation of the system architecture, data flow, and design rationale is available here:

➡️ **[docs/architecture.md](docs/architecture.md)**

---

## Repository Structure

```
energy-decision-support
│
├── README.md
│
├── docs
│   ├── architecture.md
│   └── architecture-diagram.png
│
├── docker
│   └── docker-compose.yaml
│
├── ingestion
│   └── batch
│       ├── ingest_entsoe.py
│       ├── db.py
│       ├── Dockerfile
│       └── requirements.txt
│
├── orchestration
│   └── kestra
│       └── energy_dss_pipeline.yml
│
├── warehouse
│   ├── raw
│   ├── clean
│   ├── admin
│   ├── dq
│   ├── analytics
│   ├── mart
│   └── schema.sql
│
├── scripts
│   └── deploy_kestra_flow.sh
│
└── LICENSE
```

---

## Pipeline Workflow

The pipeline is orchestrated using Kestra and executes the following stages:

1. **Ingestion**
   - Retrieve electricity demand data from ENTSO-E API
   - Parse XML into structured records
   - Load into `energy_load_raw`

2. **Clean Layer**
   - Filter invalid records
   - Normalize dataset into `energy_load_clean`

3. **Data Quality Layer**
   - Detect missing hours and time gaps
   - Validate load values
   - Generate pipeline status indicators

4. **Analytics Layer**
   - Daily summaries
   - Load anomaly detection
   - Load curve profiling

5. **Mart Layer**
   - Aggregate system-level metrics
   - Produce decision-ready indicators

6. **Validation**
   - Enforce DQ assertions
   - Fail pipeline if critical checks fail

---

## Running the System

Clone the repository:

```bash
git clone https://github.com/AsherJD-io/energy-decision-support.git
cd energy-decision-support
```

Create environment file:

```bash
cp .env.example .env
```

Start services:

```bash
docker compose -f docker/docker-compose.yaml up -d
```

Deploy Kestra flow:

```bash
bash scripts/deploy_kestra_flow.sh
```

Run pipeline:

1. Open Kestra UI: http://localhost:8087  
2. Navigate to `energy.energy_dss_pipeline`  
3. Click **Execute**

This pipeline will:

- ingest ENTSO-E data  
- build warehouse layers  
- validate data quality  
- produce analytical outputs  

---

## Roadmap

### Short Term
- stabilize batch ingestion and DQ layer
- improve pipeline observability and logging
- refine analytical views for decision support

### Mid Term
- migrate warehouse to BigQuery
- introduce dbt for transformation management
- implement cloud-based orchestration

### Long Term
- evolve into hybrid batch + streaming architecture
- integrate real-time ingestion (Kafka / Redpanda)
- introduce streaming processing (Flink or streaming database)
- enable real-time decision support layer

---

## License

This project is licensed under the **MIT License**.

See the `LICENSE` file for details.
