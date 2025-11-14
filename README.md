# travel-hospitality-data-pipeline-databricks
End-to-end Databricks ETL Pipeline using Medallion Architecture, Delta Lake, PySpark, Metadata-driven Incremental Ingestion, and SCD2 Customer Dimension.


This project simulates a real-world Travel & Hospitality Booking System where daily customer and booking data arrives as CSV files.
The goal is to build a fully automated, fault-tolerant, and production-ready data pipeline using:
Databricks
Delta Lake
PySpark
Unity Catalog
PyDeequ (Data Quality)
SCD Type 2
Metadata-driven incremental ingestion

This pipeline ensures:
✔ No missed daily files
✔ Handles late-arriving data
✔ Maintains historical changes in customer information
✔ ACID-compliant Delta tables
✔ Daily automated runs through Databricks Workflows

 Architecture — Medallion Design
                ┌──────────────────────────┐
                │        BRONZE            │
                │ Raw CSV files in DBFS    │
                │ bookings_YYYY-MM-DD.csv  │
                │ customers_YYYY-MM-DD.csv │
                └───────────┬──────────────┘
                            │ Ingest
                            ▼
                ┌──────────────────────────┐
                │          SILVER          │
                │ Clean + DQ + SCD2 Merge  │
                │ customer_scd (Delta)     │
                └───────────┬──────────────┘
                            │ Join + Agg
                            ▼
                ┌──────────────────────────┐
                │          GOLD            │
                │ booking_fact (Delta)     │
                │ Aggregated Fact Table    │
                └──────────────────────────┘

Key Features
✅ 1. Metadata-Driven Incremental Ingestion

A Delta table (pipeline_metadata) tracks last_processed_date

Pipeline auto-detects unprocessed files in DBFS

No file is ever skipped

Late-arriving files automatically processed

Avoids duplicates & reprocessing

✅ 2. SCD Type 2 Customer Dimension

Implemented using Delta Lake MERGE:

Tracks historical changes in customer profile

Maintains valid_from, valid_to

Closes old record and inserts new version

Fully ACID compliant

✅ 3. Gold-Layer Booking Fact Table

Daily booking data is transformed and aggregated:

Total revenue

Total quantity

Per customer + booking type metrics

Stored as a Delta table — optimized for reporting and BI dashboards.

✅ 4. Data Quality (PyDeequ)

Before processing data:

Row count checks

Uniqueness checks

Completeness checks

Non-negative numeric validation

Pipeline fails fast if DQ fails.
Ensures only clean data enters Silver/Gold layers.

✅ 5. Databricks Workflows (Automation)

Daily scheduled run

Automatic cluster management

Log tracking + alerting

Parameterless execution (metadata controls ingestion)

📁 Project Structure
travel-hospitality-data-pipeline-databricks/
│
├── README.md
├── architecture-diagram.png
│
├── notebooks/
│     └── incremental_booking_data_processing.py
│
├── metadata/
│     └── pipeline_metadata.sql
│
├── sample_data/
│     ├── bookings_2024-07-26.csv
│     └── customers_2024-07-26.csv
│
└── scripts/
      └── create_catalog_and_tables.sql

🔧 How the Pipeline Works (Step-by-Step)
1️⃣ Daily CSV files land in DBFS (Bronze)

Example:

dbfs:/DataEngineering/bookings_daily_data/bookings_2024-07-26.csv
dbfs:/DataEngineering/customers_daily_data/customers_2024-07-26.csv

2️⃣ Metadata table stores last processed date
CREATE TABLE IF NOT EXISTS gds_de_bootcamp.default.pipeline_metadata (
    table_name STRING,
    last_processed_date DATE
);


Example record:

booking_customer_pipeline | 1900-01-01

3️⃣ Pipeline reads list of all files → picks only missing ones

Steps:

List all files

Extract dates from filenames

Compare with metadata

Process only dates > last_processed_date

4️⃣ Apply Data Quality (PyDeequ)

If fails → stop run.

5️⃣ Apply Transformations & SCD2 Merge (Silver Layer)

Customer records undergo:

Change detection

SCD2 merge

Insert new historical version

6️⃣ Generate Aggregated Fact Table (Gold Layer)

Using PySpark aggregations.

7️⃣ Update Metadata Table

After each successful run:

UPDATE pipeline_metadata
SET last_processed_date = '<last_date_processed>'

🛠️ Technologies Used
Layer	Technology
Storage	DBFS, Delta Lake
Compute	Databricks, Spark 3.x
Orchestration	Databricks Workflows
ETL	PySpark
DQ Framework	PyDeequ
Architecture	Medallion (Bronze/Silver/Gold)
Dimension Modeling	SCD Type 2
Catalog	Unity Catalog
🚀 How to Run This Project
1️⃣ Upload sample CSV files to DBFS
dbfs:/DataEngineering/bookings_daily_data/
dbfs:/DataEngineering/customers_daily_data/

2️⃣ Create metadata table (SQL file included)

Run:

metadata/pipeline_metadata.sql

3️⃣ Run the main ETL notebook

notebooks/incremental_booking_data_processing.py

4️⃣ Schedule Daily Run

Create a Databricks Workflow:

Task: Run the notebook

Cluster: Existing or new

Schedule: Daily

No parameters required (metadata-driven)

📊 Final Output Delta Tables
🥇 Gold Layer
gds_de_bootcamp.default.booking_fact

🥈 Silver Layer
gds_de_bootcamp.default.customer_scd

📌 Metadata Layer
gds_de_bootcamp.default.pipeline_metadata

🌟 Show Support

If you liked this project:

⭐ Star the repository
🔁 Share on LinkedIn
🎯 Fork and try adding new features


Contact
Feel free to reach out:
Raj Kamal
Data Engineer
rrajkamal1999@gmail.com
