# Data Dictionary

## Bronze Layer Schema

**Table**: `yellow_taxi_bronze`

| Column | Type | Description | Nullable |
|--------|------|-------------|----------|
| VendorID | Integer | Taxi vendor identifier | Yes |
| tpep_pickup_datetime | String/Timestamp | Pickup timestamp | Yes |
| tpep_dropoff_datetime | String/Timestamp | Dropoff timestamp | Yes |
| passenger_count | Integer | Number of passengers | Yes |
| trip_distance | Double | Trip distance in miles | Yes |
| RatecodeID | Integer | Rate code identifier | Yes |
| store_and_fwd_flag | String | Store and forward flag | Yes |
| PULocationID | Integer | Pickup location ID | Yes |
| DOLocationID | Integer | Dropoff location ID | Yes |
| payment_type | Integer | Payment type code | Yes |
| fare_amount | Double | Base fare amount | Yes |
| extra | Double | Extra charges | Yes |
| mta_tax | Double | MTA tax | Yes |
| tip_amount | Double | Tip amount | Yes |
| tolls_amount | Double | Tolls amount | Yes |
| improvement_surcharge | Double | Improvement surcharge | Yes |
| total_amount | Double | Total trip amount | Yes |
| congestion_surcharge | Double | Congestion surcharge | Yes |
| airport_fee | Double | Airport fee | Yes |
| ingestion_ts | Timestamp | Data ingestion timestamp | Yes |
| source_file | String | Source file path | Yes |
| trip_date | Date | Partition column (derived) | Yes |

**Partitioning**: `trip_date` (if enabled)

---

## Silver Layer Schema

**Table**: `yellow_taxi_silver`

| Column | Type | Description | Nullable |
|--------|------|-------------|----------|
| vendorid | Integer | Taxi vendor identifier | Yes |
| tpep_pickup_datetime | Timestamp | Pickup timestamp | Yes |
| tpep_dropoff_datetime | Timestamp | Dropoff timestamp | Yes |
| passenger_count | Integer | Number of passengers | Yes |
| trip_distance | Double | Trip distance in miles | Yes |
| ratecodeid | Integer | Rate code identifier | Yes |
| store_and_fwd_flag | String | Store and forward flag (lowercase) | Yes |
| pulocationid | Integer | Pickup location ID | Yes |
| dolocationid | Integer | Dropoff location ID | Yes |
| payment_type | Integer | Payment type code | Yes |
| fare_amount | Double | Base fare amount | Yes |
| extra | Double | Extra charges | Yes |
| mta_tax | Double | MTA tax | Yes |
| tip_amount | Double | Tip amount | Yes |
| tolls_amount | Double | Tolls amount | Yes |
| improvement_surcharge | Double | Improvement surcharge | Yes |
| total_amount | Double | Total trip amount | Yes |
| congestion_surcharge | Double | Congestion surcharge | Yes |
| airport_fee | Double | Airport fee | Yes |
| ingestion_ts | Timestamp | Data ingestion timestamp | Yes |
| source_file | String | Source file path | Yes |
| trip_date | Date | Partition column (derived) | Yes |

**Partitioning**: `trip_date` (if enabled)

**Data Quality Filters Applied**:
- `trip_distance > 0`
- `fare_amount >= 0`
- `total_amount >= 0`
- Non-null pickup/dropoff datetimes

**Deduplication**: Based on `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `vendorid`, `total_amount`

---

## Gold Layer Schemas

### Daily KPIs Table

**Table**: `daily_kpis`

| Column | Type | Description | Nullable |
|--------|------|-------------|----------|
| trip_date | Date | Date of trip | No |
| daily_trip_count | Long | Number of trips per day | No |
| daily_total_revenue | Double | Total revenue per day | No |
| avg_trip_distance | Double | Average trip distance per day | Yes |
| avg_passenger_count | Double | Average passenger count per day | Yes |

**Partitioning**: `trip_date` (if enabled)

**Aggregation**: Grouped by `trip_date`

---

### Zone Demand Table

**Table**: `zone_demand`

| Column | Type | Description | Nullable |
|--------|------|-------------|----------|
| trip_date | Date | Date of trip | No |
| pu_location_id | Integer | Pickup location ID | Yes |
| trip_count | Long | Number of trips per zone per day | No |
| total_revenue | Double | Total revenue per zone per day | No |

**Partitioning**: `trip_date` (if enabled)

**Aggregation**: Grouped by `trip_date` and `pu_location_id`

---

## Metadata Tables

### DQ Metrics Table

**Table**: `dq_metrics`

| Column | Type | Description |
|--------|------|-------------|
| run_id | String | Pipeline run identifier |
| layer | String | Layer name (bronze, silver, gold) |
| check_name | String | DQ check name |
| description | String | Check description |
| violation_count | Long | Number of violations found |
| expected_result | String | Expected result description |
| severity | String | Severity level (ERROR, WARNING, INFO) |
| passed | Boolean | Whether check passed |
| error | String | Error message (if any) |
| timestamp | Timestamp | Check execution timestamp |
| run_timestamp | Timestamp | Pipeline run timestamp |

---

### Pipeline Runs Table

**Table**: `pipeline_runs`

| Column | Type | Description |
|--------|------|-------------|
| run_id | String | Unique run identifier |
| job_name | String | Job name (bronze, silver, gold, all) |
| layer | String | Layer name |
| status | String | Execution status (running, completed, failed) |
| start_time | Timestamp | Job start time |
| end_time | Timestamp | Job end time |
| duration_seconds | Double | Execution duration |
| initial_row_count | Long | Initial row count |
| final_row_count | Long | Final row count |
| rows_filtered | Long | Rows filtered out |
| rows_deduplicated | Long | Rows deduplicated |
| error_message | String | Error message (if failed) |
| config_snapshot | String | Configuration JSON snapshot |
| metadata | String | Additional metadata JSON |
| created_at | Timestamp | Record creation timestamp |

---

## Column Naming Conventions

- **Bronze**: Preserves original CSV column names (mixed case)
- **Silver**: Standardized to lowercase with underscores
- **Gold**: Descriptive names following analytics conventions

## Data Types

- **Timestamps**: Stored as Spark TimestampType
- **Dates**: Stored as Spark DateType (for partitioning)
- **Numeric**: DoubleType for amounts, IntegerType for counts/IDs
- **Strings**: StringType with trimming and lowercasing in Silver

