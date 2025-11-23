# NYC Taxi Data Lakehouse ETL Pipeline

A production-style PySpark-based ETL pipeline implementing a **Bronze → Silver → Gold** Data Lakehouse architecture using Delta Lake and NYC Yellow Taxi Trip data.

## Overview

This project demonstrates a complete Data Lakehouse ETL pipeline with three layers:

- **Bronze Layer (Raw Zone)**: Ingests raw CSV data with minimal transformation, adding metadata columns for tracking
- **Silver Layer (Clean Zone)**: Applies data quality filters, type casting, and deduplication to create clean, conformed data
- **Gold Layer (Analytics Zone)**: Creates aggregated tables for analytics (daily KPIs, zone-level demand metrics)

## Project Structure

```
lakehouse_project/
├── config/
│   └── config.yaml              # Configuration file with paths and settings
├── data/
│   └── raw/                     # Place sample CSV files here
├── lakehouse/
│   ├── bronze/                  # Bronze Delta tables (auto-created)
│   ├── silver/                  # Silver Delta tables (auto-created)
│   └── gold/                    # Gold Delta tables (auto-created)
├── etl/
│   ├── __init__.py
│   ├── bronze_job.py            # Bronze layer ETL logic
│   ├── silver_job.py            # Silver layer ETL logic
│   └── gold_job.py              # Gold layer ETL logic
├── utils/
│   ├── __init__.py
│   ├── spark.py                 # Spark session utilities
│   └── logging_utils.py         # Logging configuration
├── main.py                      # Main orchestration script
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## Prerequisites

- Python 3.8 or higher
- Java 8 or higher (required for Spark)
- Local Spark installation (or Spark will be downloaded via PySpark)

## Setup

### 1. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Prepare Sample Data

Download or place NYC Yellow Taxi Trip data CSV files in the `data/raw/` directory.

**Expected CSV Schema:**
- `tpep_pickup_datetime` (timestamp)
- `tpep_dropoff_datetime` (timestamp)
- `passenger_count` (integer)
- `trip_distance` (double)
- `PULocationID` (integer)
- `DOLocationID` (integer)
- `fare_amount` (double)
- `extra` (double)
- `mta_tax` (double)
- `tip_amount` (double)
- `tolls_amount` (double)
- `total_amount` (double)
- `payment_type` (integer)
- `VendorID` (integer)
- `RatecodeID` (integer)
- `store_and_fwd_flag` (string)

**Sample Data Sources:**
- NYC Taxi & Limousine Commission: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Sample files are available for download (e.g., `yellow_tripdata_2023-01.csv`)

Place one or more CSV files in `data/raw/` directory. The pipeline will read all CSV files in that directory.

## Usage

### Run Individual Jobs

```bash
# Run Bronze job only (ingest raw CSV to Delta)
python main.py --job bronze

# Run Silver job only (clean and deduplicate)
python main.py --job silver

# Run Gold job only (create aggregations)
python main.py --job gold
```

### Run Complete Pipeline

```bash
# Run all jobs in sequence: Bronze → Silver → Gold
python main.py --job all
```

### Custom Configuration

```bash
# Use a custom config file
python main.py --job all --config path/to/custom_config.yaml
```

## Layer Schemas

### Bronze Layer

Raw data with added metadata:
- All original columns from CSV
- `ingestion_ts` (timestamp): When the record was ingested
- `source_file` (string): Source file path

**Write Mode:** `overwrite` (for demo purposes; production might use `append` with partitioning)

### Silver Layer

Cleaned and conformed data:
- All columns standardized to lowercase with underscores
- Proper data types (timestamps, integers, doubles)
- Data quality filters applied:
  - `trip_distance > 0`
  - `fare_amount >= 0`
  - `total_amount >= 0`
  - Non-null pickup/dropoff datetimes
- Deduplication based on: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `vendorid`, `total_amount`

### Gold Layer

Two aggregated tables:

#### 1. Daily KPIs (`lakehouse/gold/daily_kpis`)
- `trip_date` (date): Date of trip
- `daily_trip_count` (long): Number of trips per day
- `daily_total_revenue` (double): Sum of total_amount per day
- `avg_trip_distance` (double): Average trip distance per day
- `avg_passenger_count` (double): Average passenger count per day

#### 2. Zone Demand (`lakehouse/gold/zone_demand`)
- `trip_date` (date): Date of trip
- `pu_location_id` (integer): Pickup location ID
- `trip_count` (long): Number of trips per zone per day
- `total_revenue` (double): Total revenue per zone per day

## Configuration

Edit `config/config.yaml` to customize:
- Input/output paths
- Data quality thresholds
- Deduplication columns
- Spark settings

## Logging

Logs are written to stdout with timestamps, log levels, and module names. Each job logs:
- Start/end times
- Row counts before/after transformations
- Filter conditions applied
- Error messages (if any)

## Example Workflow

```bash
# 1. Setup environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Place CSV file(s) in data/raw/
# e.g., data/raw/yellow_tripdata_2023-01.csv

# 3. Run complete pipeline
python main.py --job all

# 4. Explore Delta tables (optional)
# You can use Spark SQL or Delta Lake tools to query the tables
```

## Notes

- **Write Modes**: Currently using `overwrite` mode for simplicity. In production, consider `append` mode with partitioning (e.g., by date) for incremental loads.
- **Schema Evolution**: Delta Lake supports schema evolution. The pipeline uses `overwriteSchema: true` to allow schema changes.
- **Case Sensitivity**: The code handles case-insensitive column matching to accommodate variations in CSV headers.
- **Error Handling**: Each job includes error handling and logging. Failures will be logged with stack traces.

## Troubleshooting

1. **Java Not Found**: Ensure Java 8+ is installed and `JAVA_HOME` is set
2. **Delta Lake Errors**: Verify `delta-spark` version is compatible with `pyspark` version
3. **File Not Found**: Ensure CSV files are in `data/raw/` directory
4. **Memory Issues**: Adjust Spark memory settings in `utils/spark.py` if needed

## License

See LICENSE file for details.
