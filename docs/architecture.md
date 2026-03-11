# Energy Decision Support System — Architecture

![System Architecture](architecture-diagram.png)

---

## 1. Overview

This project implements an Energy Decision Support System (DSS) designed to support analytical and planning decisions in electricity systems.

The architecture separates data ingestion, validation, storage, and analytical processing into distinct layers. This layered structure improves transparency, reproducibility, and reliability of analytical results.

---

## 2. System Objectives

The system is designed to:

- ingest electricity demand data from an authoritative external source
- store raw observations in a structured warehouse
- validate the integrity of time-series data
- produce analytical outputs that support energy system analysis

---

## 3. Data Source

The system uses electricity demand data from the **ENTSO-E Transparency Platform**, which publishes standardized electricity system data for the European power grid.

The pipeline specifically ingests **Actual Total Load** observations representing realized electricity demand at hourly resolution.

---

## 4. Architectural Layers

The system follows a layered analytical warehouse architecture.

```
Raw Layer
   ↓
Quality Validation Layer
   ↓
Clean Analytical Layer
   ↓
Decision Support Views
```

Each layer progressively improves data reliability and analytical usability.

---

### 4.1 Ingestion Layer

The ingestion layer retrieves electricity demand data from the ENTSO-E API.

Key characteristics:

- batch ingestion with explicit time windows
- containerized execution using Docker
- parameterized execution using environment variables

The ingestion process parses XML responses and inserts structured records into the warehouse.

---

### 4.2 Storage Layer

The storage layer consists of a PostgreSQL analytical warehouse.

Raw observations are stored in the table:

```
energy_load_raw
```

This table preserves the original temporal resolution and values provided by the external data source.

---

### 4.3 Data Quality Layer

The system performs explicit validation of the dataset before analytical processing.

Quality checks include:

- time-series continuity
- missing hourly observations
- invalid electricity demand values

Validation views include:

```
dq_time_gaps
dq_missing_hours
dq_invalid_loads
```

These views identify structural problems in the dataset.

---

### 4.4 Clean Analytical Layer

Validated observations are stored in the analytical base table:

```
energy_load_clean
```

Records in this table must satisfy:

- non-null demand values
- non-negative load values
- unique hourly records

This table represents the **trusted dataset for analytical queries**.

---

### 4.5 Decision Support Layer

The analytical layer exposes operational insights through SQL views.

Key analytical outputs include:

```
daily_load_summary
hourly_load_anomalies
daily_load_curve_profile
```

These views transform hourly demand observations into interpretable operational indicators.

---

## 5. System Data Flow

```
ENTSO-E Transparency Platform
            │
            ▼
     Batch Ingestion
     (Python Parser)
            │
            ▼
      energy_load_raw
            │
            ▼
     Data Quality Validation
            │
            ▼
      energy_load_clean
            │
            ▼
      Analytical Views
```

Each stage progressively improves the reliability and interpretability of the dataset.

---

## 5.1 Analytical Warehouse Model

The DSS implements a layered warehouse structure commonly used in time-series analytics platforms.

### Raw Layer
Stores electricity demand data exactly as received from the source API.

### Clean Layer
Contains validated hourly observations where invalid or inconsistent records have been removed.

### Analytics Layer
Contains derived analytical views used for operational interpretation.

This structure ensures:

- traceability of source data
- reproducibility of analytical outputs
- reliability of downstream queries

---

## 6. Design Rationale

The architecture emphasizes:

- **separation of concerns** between ingestion, storage, and analytics  
- **reproducibility** through containerized infrastructure  
- **extensibility** for future analytical layers and orchestration  
- **analytical transparency** through SQL-based transformations  

---

## 7. Future Extensions

Potential system extensions include:

- workflow orchestration
- additional transformation layers
- demand forecasting models
- real-time or near-real-time ingestion

---

## 8. External Data Source Availability

The system integrates with the ENTSO-E Transparency Platform Web API.

During development, archived XML responses were used to allow pipeline development before live API credentials were activated.

Once API access is enabled, ingestion can switch to live mode without modification to:

- database schemas
- downstream transformations
- analytical outputs
