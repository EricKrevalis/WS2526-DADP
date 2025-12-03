# **Hybrid Data Lakehouse Architecture**

## **1\. High-Level Overview**

This project implements a **Hybrid Cloud/Local Data Lakehouse**. It leverages serverless cloud infrastructure for robust, 24/7 data ingestion, while keeping the heavy processing and storage costs zero by utilizing local hardware for the "Lakehouse" logic.

### **Architectural Diagram**

graph LR  
    subgraph "AWS Cloud (Always On)"  
        Lambda\[AWS Lambda\\n(Scrapers)\] \--\>|JSONL| S3\[(S3 Bucket\\n'Raw Data')\]  
    end

    subgraph "Local Machine (On Demand)"  
        S3 \--\>|Pull Batch| KC\[Kafka Connect\\n(S3 Source)\]  
        KC \--\>|Stream| Kafka\[Kafka Broker\]  
        Kafka \--\>|Read Stream| Spark\[Apache Spark\\n(Processing)\]  
          
        Spark \--\>|Write| Silver\[(Local Disk\\n/datalake/silver\\nParquet Files)\]  
        Silver \--\>|Query| DuckDB\[DuckDB\\n(OLAP Engine)\]  
        DuckDB \--\>|Viz| Streamlit\[Streamlit/Dashboard\]  
    end

## **2\. Ingestion Layer (Remote / Cloud)**

This layer runs 24/7. Its only responsibility is to capture data reliably and persist it to object storage.

* **Compute:** AWS Lambda (Python 3.x)  
* **Schedule:** Every 10 minutes (via EventBridge Scheduler)  
* **Storage (Bronze Remote):** AWS S3  
* **Format:** JSON Lines (.jsonl)  
* **Cost Strategy:** Zero fixed costs. Uses AWS Free Tier for requests/compute.

**Data Flow:**

1. **TomTom Scraper:** Hits API \-\> Flattens Grid (45 pts) \-\> Uploads raw/tomtom/YYYY/MM/DD/file.jsonl  
2. **Deutsche Bahn:** Hits API \-\> Extracts Departures \-\> Uploads raw/db/YYYY/MM/DD/file.jsonl  
3. **OpenWeather:** Hits API \-\> Flattens Grid \-\> Uploads raw/owm/YYYY/MM/DD/file.jsonl

## **3\. Streaming Layer (Local / Catch-Up)**

This layer decouples the cloud storage from the local processing engine. It allows the local machine to be offline for days and "catch up" instantly upon boot.

* **Technology:** Docker (Confluent Platform)  
* **Connector:** kafka-connect-s3-source  
* **Mechanism:**  
  * On docker-compose up, the connector scans the S3 bucket.  
  * It identifies all new files created since the last run.  
  * It pulls the JSON content and pushes it into Kafka Topics (raw-db, raw-traffic, raw-weather).

## **4\. Processing & Storage Layer (Local Lakehouse)**

This is where the "Big Data" work happens. We use the **Medallion Architecture** (Bronze, Silver, Gold).

### **A. Bronze Layer (Raw)**

* **Location:** AWS S3 (Primary) & Kafka Topics (Ephemeral).  
* **Content:** The raw, unadulterated JSON strings from the scrapers.  
* **Purpose:** Audit trail. If our processing code has a bug, we can always re-play the Bronze data from S3.

### **B. Silver Layer (Cleansed & Partitioned)**

* **Compute:** PySpark (Structured Streaming or Batch).  
* **Action:**  
  * Reads from Kafka.  
  * Parses JSON strings into Columns.  
  * Casts Types (String \-\> Timestamp, String \-\> Double).  
  * Deduplicates rows (if S3 sent the same bus twice).  
* **Storage:** Local Filesystem (./datalake/silver/)  
* **Format:** **Parquet** (Columnar, Compressed).  
* **Partitioning:** By City and Date.  
  * datalake/silver/traffic/city=Hamburg/date=2023-10-27/part-001.parquet

### **C. Gold Layer (Aggregated & Business Logic)**

* **Compute:** PySpark or DuckDB.  
* **Action:**  
  * **Joins:** Merges Traffic and Weather data on grid\_id and timestamp (using 10-min windows).  
  * **Aggregations:** "Average congestion per city per hour", "Correlation between Rain and Delay".  
* **Storage:** Local Filesystem (./datalake/gold/) or DuckDB Table.  
* **Format:** Parquet.

## **5\. Analytics & Serving (The Database)**

Instead of running a heavy PostgreSQL instance, we use **DuckDB**.

* **Why DuckDB?**  
  * It is an "In-Process" OLAP database (runs inside your Python script).  
  * It can query **Parquet files directly** without loading them (Zero-Copy).  
  * It is incredibly fast for analytical queries (Group Bys, Joins).

**Example Workflow:**

1. **Spark** writes clean Parquet files to ./datalake/silver/traffic/.  
2. **Analysis Script (Python/DuckDB):**

import duckdb

\# Query the local lake directly using SQL  
df \= duckdb.query("""  
    SELECT   
        city,   
        AVG(congestion\_ratio) as avg\_congestion  
    FROM './datalake/silver/traffic/\*/\*.parquet'  
    WHERE current\_speed \< free\_flow\_speed  
    GROUP BY city  
""").to\_df()  
