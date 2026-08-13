# Ride-Sharing Analytics & Driver Performance Pipeline

## Project overview
This project implements a PySpark data engineering pipeline for ride-sharing analytics using a Medallion Architecture:

**Bronze → Silver → Gold**

The supplied project brief requires tracking ride/driver activity, cancellations, delays, high-demand locations, driver performance and business KPIs.

## Dataset
Three CSV files are used:
- `drivers.csv`: driver_id, name, city, rating
- `trips.csv`: trip_id, driver_id, pickup_location, drop_location, distance_km, fare_amount, trip_status
- `trip_logs.csv`: log_id, trip_id, start_time, end_time, delay_minutes, cancellation_flag

## Architecture

### Bronze
Raw CSV data is read with PySpark and written to Parquet without business transformations.

### Silver
The datasets are joined and cleaned. The Silver layer:
- removes duplicate IDs;
- validates IDs and numeric ranges;
- parses timestamps;
- validates trip status and cancellation flags;
- calculates trip duration;
- creates completion/cancellation/delay flags;
- calculates recognized revenue from completed rides.

**Important dataset-specific rule:** cancelled trips in the supplied data have missing `end_time`. They are retained because cancellation analysis is a required project objective. Completed trips must have an `end_time`.

### Gold
Business-ready Parquet tables:
- `overall_kpi`
- `driver_performance`
- `cancellation_by_driver`
- `pickup_demand`
- `delay_analysis`
- `revenue_by_city`

## Window function
Drivers are ranked using `row_number()` with:
1. completion rate descending;
2. cancellation rate ascending;
3. driver rating descending;
4. total revenue descending.

This provides a transparent performance ranking without collecting the full dataset into Python.

## Spark optimization
The implementation uses:
- broadcast join for the small driver dimension;
- column pruning;
- caching of the reused Silver DataFrame;
- Adaptive Query Execution;
- Parquet columnar storage;
- Spark window functions.

## Results from the supplied data
- Total trips: **150**
- Completed trips: **63**
- Cancelled trips: **87**
- Completion rate: **42.00%**
- Cancellation rate: **58.00%**
- Total recognized revenue: **10091.08**
- Average delay: **4.77 minutes**
- Average completed-trip duration: **33.48 minutes**
- Highest-demand pickup location: **Airport (36 trips)**
- Highest-revenue pickup location: **IT Park (3191.85)**
- Highest-revenue city: **Delhi (2851.14)**

## How to run

### Local Spark
Install Java and Python first, then:

```bash
pip install -r requirements.txt
spark-submit src/ride_sharing_pipeline.py
```

### Jupyter / Google Colab
Open `notebooks/ride_sharing_pipeline.ipynb`.

If PySpark is unavailable:
```python
%pip install pyspark
```

Then execute the notebook from top to bottom.

## Expected output structure

```text
output/
├── bronze/
│   ├── drivers/
│   ├── trips/
│   └── trip_logs/
├── silver/
│   └── trips_enriched/
└── gold/
    ├── overall_kpi/
    ├── driver_performance/
    ├── cancellation_by_driver/
    ├── pickup_demand/
    ├── delay_analysis/
    └── revenue_by_city/
```

## Project deliverables
1. PySpark notebook
2. PySpark source script
3. Bronze/Silver/Gold Parquet outputs after execution
4. README documentation
5. Source CSV datasets
