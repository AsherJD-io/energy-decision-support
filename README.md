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
├── warehouse
│   └── schema.sql
│
├── notebooks
│
├── pipeline.py
│
└── LICENSE
```

---

## Pipeline Workflow

The pipeline processes electricity demand data through the following stages:

1. Retrieve hourly demand data from the ENTSO-E API  
2. Parse XML responses into structured records  
3. Load observations into a PostgreSQL warehouse  
4. Apply data quality validation checks  
5. Generate analytical views for decision support  

---

## Running the System

Start the local environment:

```bash
docker compose -f docker/docker-compose.yaml up
```

Run the ingestion pipeline:

```bash
docker compose -f docker/docker-compose.yaml run --rm ingestion
```

This will:

1. retrieve ENTSO-E electricity demand data  
2. parse the XML responses  
3. insert validated records into PostgreSQL  

---

## Future Evolution

Planned extensions include:

- deployment to **Google Cloud Platform (GCP)**
- automated pipeline scheduling
- expanded analytical indicators
- potential real-time demand monitoring

---

## License

This project is licensed under the **MIT License**.

See the `LICENSE` file for details.
