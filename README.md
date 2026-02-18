# MarketOps

MarketOps is an open market research operations platform for tracking and managing market assets and objects, executing ETL pipelines, automation, monitoring, simulation and prediction. It uses a hybrid Python/Java architecture to handle cataloging of assets, full data lineage, permissioned control, and easy extension for ETL/data science pipelines.

---

## Features

- **Object/Aggregate Tracking** – Manage and catalog market-related assets, sources, models, and datasets.
- **ETL Pipelines** – Register and orchestrate Extract-Transform-Load (ETL) jobs.
- **Monitoring** – Track ETL, simulation, and asset status.
- **Simulation & Prediction** – Register models and predictive runs with full lineage.
- **Data Lineage** – Automatically trace flows from raw source → transformation → dataset/model → prediction.
- **API Governance** – Java REST API server with permissioned endpoints for CRUD operations on cataloged entities.

---

## Technology

- **Backend**: Java 17+, Javalin & Hibernate, PostgreSQL
- **Pipeline & Data Science**: Python 3, using decorator pattern for catalog integration
- **Registry**: REST endpoints (`/catalog`) for registering entities, accessible via direct HTTP or Python decorators

---

## Python Decorators (`cataloglib.py`)

These decorators, provided by `MarketOpsCatalog`, instrument your data flows and automatically register all pipeline objects with the Java/Postgres backend for governance, versioning, and monitoring.

### 1. @market.DataSource

**Purpose:** Register a data source function. Automatically infers type and connection based on code analysis, but can be overridden.

**Usage Example:**
```python
from cataloglib import market

@market.DataSource(type="API", format="JSON")
def fetch_market_data():
    return requests.get("https://api.market.com/data").json()
```

- **Parameters:**  
  - `type`: `"API"`, `"FILE"`, `"CLOUD"` (default auto-detected)
  - `format`: `"JSON"`, `"CSV"`, `"PARQUET"` (default auto-detected)
  - `connectionData`: Path/URL, default auto-detected

---

### 2. @market.DataSet

**Purpose:** Register a dataset produced by a function.

**Usage Example:**
```python
@market.DataSet(name="transactions", description="All market transactions")
def clean_transactions():
    # Data cleaning code
    return ...
```

- **Parameters:**
  - `name`: Human name of dataset (default function name)
  - `description`: String (default: `"Auto-generated dataset"`)
  - `path`: Storage path (default: `"internal_registry"`)

---

### 3. @market.Model

**Purpose:** Register a predictive or analytical model.

**Usage Example:**
```python
@market.Model(name="pricing_model", parameters={"type": "regression"})
def model_train():
    # Training code
    return ...
```

- **Parameters:**
  - `name`: Model name (default function name)
  - `parameters`: Hyperparameters/config dict

---

### 4. @market.ETL

**Purpose:** Register an ETL function (Extract/Transform/Load pipeline).

**Usage Example:**
```python
@market.ETL(trigger="schedule")
def run_etl():
    # ETL logic
    return ...
```

- **Parameters:**
  - `trigger`: `"manual"` (default), `"schedule"`, `"event"`

---

### 5. @market.Lineage

**Purpose:** Register data lineage between source, asset, and model.

**Usage Example:**
```python
@market.Lineage(source_id="fetch_market_data", asset_id="asset01", model_id="modelA")
def predict():
    # Pipeline that uses source, makes asset, and uses modelA
    return ...
```

- **Parameters:**
  - `source_id`: ID of data source
  - `asset_id`: ID of downstream market asset
  - `model_id`: (optional) Model registry ID used in operation

---

## How It Works: Decorator Integration

When you use a decorator, it:
- Performs source code inspection to auto-detect type and connection.
- Registers an object/dataset/model/etl/lineage metadata via HTTP POST to the Java backend API.
- Prints a confirmation (e.g. `✅ Registered fetch_market_data to data-sources`).

---

## Example: Full Pipeline

```python
from cataloglib import market

@market.DataSource()
def source():
    ...

@market.DataSet()
def prepare(src=source()):
    ...

@market.Model()
def train(data=prepare()):
    ...

@market.ETL()
def pipeline():
    ...

@market.Lineage(source_id="source", asset_id="prepare", model_id="train")
def predict():
    ...
```

---

## Java Catalog API

The backend defines RESTful CRUD endpoints (see `/marketops.catalog.repository`). 
- Authentication uses the HTTP header `X-User` (default: `admin_user`).
- Endpoints: `/catalog/data-sources`, `/catalog/data-sets`, `/catalog/models`, `/catalog/etl`, `/catalog/lineage`, etc.
- All objects are saved to PostgreSQL and accessible via API queries.

---

## How To Use

1. **Install Python dependencies** (`pip install -r requirements.txt`)
2. **Run/post your ETL/data science code** with the provided decorators for full catalog integration.
3. **Run the Java backend** (`mvn spring-boot:run` or equivalent for Javalin/Hibernate API).
4. **Access the catalog**:
   - Via REST API endpoints (see code and port in `/marketops.catalog.repository`)
   - Or through code using the decorators, which auto-register all entities

---

## License

[Specify license]

---

## Contact/Issues

Create an issue on [GitHub Issues](https://github.com/v3nkates/Marketops/issues) for support.

---

_Last updated: 2026-02-18_
