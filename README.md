# NYC Taxi Pipeline Project - Phase 1

## Current Implementation
This is the initial phase of the NYC Taxi data pipeline project, focusing on data ingestion and validation using Apache Airflow.

### Completed Features
✅ Airflow DAG Implementation
- Monthly scheduled data ingestion from NYC Taxi & Limousine Commission API
- Incremental data loading framework
- Comprehensive error handling and logging

✅ Data Quality Validations
- Null value checks for critical fields
- Date range validations
- Business rule validations (fare amount, trip distance, pickup/dropoff time)
- Data type conversions and validations

✅ Database Structure
- Staging table for raw taxi trip data
- Processing log table for tracking data loads
- Proper data type definitions for all fields

## Technical Details
### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- PostgreSQL 14+

### Project Structure
```
nyc_taxi/
├── README.md
├── dags
│   └── nyc_taxi.py
├── dbt
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── macros
│   ├── models
│   │   └── example.sql
│   ├── profiles.yml
│   ├── seeds
│   ├── target
│   │   ├── compiled
│   │   │   └── nyc_taxi
│   │   │       └── models
│   │   │           └── example.sql
│   │   ├── graph.gpickle
│   │   ├── graph_summary.json
│   │   ├── manifest.json
│   │   ├── partial_parse.msgpack
│   │   ├── run
│   │   │   └── nyc_taxi
│   │   │       └── models
│   │   │           └── example.sql
│   │   ├── run_results.json
│   │   └── semantic_manifest.json
│   └── tests
├── docker-compose.yml
├── plugins
├── poetry.lock
└── pyproject.toml
```

### Installation & Setup

1. Start the environment:
```bash
docker compose up -d
```
2. Initialize Airflow:
```bash
# Initialize database
docker compose run airflow-webserver airflow db init

# Create admin user
docker compose run airflow-webserver airflow users create \
    --username admin \
    --firstname admin \
    --lastname user \
    --role Admin \
    --email admin@example.com \
    --password admin
```

## DAG Configuration
- Schedule: Monthly (0 0 1 * *)
- Start Date: 2024-01-01
- End Date: 2024-12-31
- Max Active Runs: 1
- Retries: 1
- Retry Delay: 5 minutes

## Data Validation Checks
1. Null Value Checks
- Pickup datetime
- Dropoff datetime
- Fare amount

2. Date Range Validations
- Min/Max pickup dates
- Min/Max dropoff dates
- Chronological order validation

3. Business Rule Validations
- Positive fare amounts
- Positive trip distances
- Valid datetime sequences

## Database Tables
1. Staging Table (taxi_trips_staging)
```sql
CREATE TABLE taxi_trips_staging (
    id SERIAL PRIMARY KEY,
    vendorid INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count FLOAT,
    trip_distance FLOAT,
    -- ... other fields ...
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
2. Processing Log (taxi_data_processing_log)
```sql
CREATE TABLE taxi_data_processing_log (
    id SERIAL PRIMARY KEY,
    processing_date TIMESTAMP,
    year_month VARCHAR(7),
    records_processed INTEGER,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Usage
1. Access Airflow UI:
- URL: http://localhost:8080
- Username: hsiaoyu
- Password: citian

2. Monitor DAG Execution:
- Check DAG status in Airflow UI
- View processing logs in taxi_data_processing_log table
- Monitor data quality validation results in logs

## Troubleshooting
### Common Issues:
1. API Connection Issues
- Check API endpoint availability
- Verify network connectivity
- Review request parameters

2. Data Quality Failures
- Check validation error messages in logs
- Review failed records in staging table
- Verify data format from source

## Next Steps
⏳ Pending Implementation:
- DBT data transformations
- Dimensional modeling
- PostgreSQL performance optimization
- Analytical views creation