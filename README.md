# Airflow Stock Pipeline

A data pipeline built with Apache Airflow that ingests financial data from public APIs (SEC EDGAR, etc.) into a structured S3 data lake following a Bronze / Silver / Gold architecture.

---

## Project Structure
```
~/airflow-stock-pipeline/
    ├── config
    ├── dags
    │   ├── bronze
    │   │   ├── market
    │   │   └── sec
    │   │       ├── dag_bronze_company_cik.py
    │   │       └── dag_bronze_company_kpis.py
    │   ├── stock_prices_daily.py
    │   └── utils
    │       └── dag_defaults.py
    ├── data
    ├── docker-compose.yaml
    ├── Dockerfile
    ├── plugins
    │   ├── fmp
    │   │   ├── client.py
    │   │   └── __init__.py
    │   └── testes
    │       ├── client.py
    │       └── __init__.py
    ├── README.md
    └── requirements.txt
```

---

## Architecture

| Layer  | Description                                      |
|--------|--------------------------------------------------|
| Bronze | Raw data ingested as-is from source APIs         |
| Silver | Cleaned, validated, and typed data               |
| Gold   | Aggregated, business-ready datasets              |

---

## DAGs
| dag_id  | filelocation                                      |
|--------|--------------------------------------------------|
| monthly_sec_company_cik  | /opt/airflow/dags/bronze/sec/dag_bronze_company_cik.py   |
| monthly_sec_company_kpis | /opt/airflow/dags/bronze/sec/dag_bronze_company_kpis.py  |
| stock_prices_daily       | /opt/airflow/dags/stock_prices_daily.py  |

---

## Setup

### Prerequisites
- Docker & Docker Compose
- AWS credentials with S3 access

### Running locally
```bash
# Clone the repo
git clone https://github.com/EduardoMoreira25/airflow-stock-pipeline.git
cd airflow-stock-pipeline

# Start Airflow and load custom image
docker compose up --build -d

# Access the UI
open http://localhost:8080  
```

### Airflow Variables

Set these in the Airflow UI under **Admin → Variables**:

| Variable                          | Description                              | Required |
|-----------------------------------|------------------------------------------|----------|
|           `ALERT_EMAIL`           | Email address to receive failure alerts  | No       |
| `SEC_COMPANY_HISTORICAL_KPIS_URL` | Email address to receive failure alerts  | No       |
|          `SEC_USER_AGENT`         | Email address to receive failure alerts  | No       |


### AWS

The pipeline writes to S3. Make sure your environment has credentials available via:
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_DEFAULT_REGION` in local .env file (see example)

---

## Development
```bash
# Create a feature branch
git checkout -b feat/your-feature

# Run a specific DAG task locally
# docker exec -it  airflow tasks test   
```

---

## Tech Stack

- [Apache Airflow](https://airflow.apache.org/)
- [SEC EDGAR API](https://www.sec.gov/developer)
- [AWS S3](https://aws.amazon.com/s3/) via `boto3`
- Docker

---

## Author

**Eduardo Moreira** — [GitHub](https://github.com/EduardoMoreira25)