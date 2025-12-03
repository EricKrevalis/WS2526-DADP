# **Local Data Lakehouse Setup Guide**

This guide explains how to set up the Local portion of our Hybrid Data Lakehouse.  
Goal: Run Kafka and Spark on your own machine to consume data from our shared AWS S3 bucket.

## **🛑 1\. Prerequisites (Install These First)**

Before running any code, ensure you have the following installed on your laptop:

### **A. Docker Desktop**

This runs our Kafka infrastructure.

* **Download:** [Docker Desktop](https://www.docker.com/products/docker-desktop/)  
* **Verify:** Open a terminal (Command Prompt/Terminal) and type docker \--version.

### **B. Python 3.9+**

We need Python for Spark and DuckDB.

* **Recommendation:** Use [Anaconda](https://www.anaconda.com/) or standard Python.  
* **Verify:** Type python \--version or python3 \--version.

### **C. Java 11 (Crucial for Spark)**

Apache Spark runs on the JVM. You **must** have Java installed, or PySpark will crash.

* **Download:** [OpenJDK 11](https://adoptium.net/temurin/releases/?version=11) (Choose JDK, not JRE).  
* **Verify:** Type java \-version.

### **D. AWS Credentials**

You need the **Access Key ID** and **Secret Access Key** for the IAM User TrafficDataUser (Ask the project lead if you don't have these).

## **📂 2\. Project Folder Structure**

Create a folder named traffic-data-lake and set up this exact structure:

traffic-data-lake/  
├── config/  
│   └── s3\_source.json       \<-- Kafka Connector Config  
├── datalake/                \<-- Where data is saved locally  
│   ├── bronze/  
│   ├── silver/  
│   └── gold/  
├── kafka-data/              \<-- Docker volumes (auto-created, do not touch)  
├── scripts/  
│   └── process\_stream.py    \<-- PySpark script  
├── docker-compose.yaml      \<-- Infrastructure definition  
├── requirements.txt         \<-- Python dependencies  
└── .env                     \<-- Your secrets (NEVER commit this to Git)

## **⚙️ 3\. Configuration Files**

Create these files in your project folder.

### **A. docker-compose.yaml (In root)**

This defines our streaming platform.

version: '2'  
services:  
  zookeeper:  
    image: confluentinc/cp-zookeeper:7.5.0  
    hostname: zookeeper  
    container\_name: zookeeper  
    ports: \["2181:2181"\]  
    environment:  
      ZOOKEEPER\_CLIENT\_PORT: 2181  
      ZOOKEEPER\_TICK\_TIME: 2000  
    volumes:  
      \- ./kafka-data/zookeeper:/var/lib/zookeeper/data

  broker:  
    image: confluentinc/cp-server:7.5.0  
    hostname: broker  
    container\_name: broker  
    depends\_on: \[zookeeper\]  
    ports: \["9092:9092", "9101:9101"\]  
    environment:  
      KAFKA\_BROKER\_ID: 1  
      KAFKA\_ZOOKEEPER\_CONNECT: 'zookeeper:2181'  
      KAFKA\_LISTENER\_SECURITY\_PROTOCOL\_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT\_HOST:PLAINTEXT  
      KAFKA\_ADVERTISED\_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT\_HOST://localhost:9092  
      KAFKA\_OFFSETS\_TOPIC\_REPLICATION\_FACTOR: 1  
      CONFLUENT\_METRICS\_REPORTER\_TOPIC\_REPLICATION\_FACTOR: 1  
    volumes:  
      \- ./kafka-data/kafka:/var/lib/kafka/data

  connect:  
    image: confluentinc/cp-kafka-connect:7.5.0  
    hostname: connect  
    container\_name: connect  
    depends\_on: \[broker\]  
    ports: \["8083:8083"\]  
    environment:  
      CONNECT\_BOOTSTRAP\_SERVERS: 'broker:29092'  
      CONNECT\_REST\_ADVERTISED\_HOST\_NAME: connect  
      CONNECT\_GROUP\_ID: compose-connect-group  
      CONNECT\_CONFIG\_STORAGE\_TOPIC: docker-connect-configs  
      CONNECT\_CONFIG\_STORAGE\_REPLICATION\_FACTOR: 1  
      CONNECT\_OFFSET\_STORAGE\_TOPIC: docker-connect-offsets  
      CONNECT\_OFFSET\_STORAGE\_REPLICATION\_FACTOR: 1  
      CONNECT\_STATUS\_STORAGE\_TOPIC: docker-connect-status  
      CONNECT\_STATUS\_STORAGE\_REPLICATION\_FACTOR: 1  
      CONNECT\_KEY\_CONVERTER: org.apache.kafka.connect.storage.StringConverter  
      CONNECT\_VALUE\_CONVERTER: org.apache.kafka.connect.storage.StringConverter  
      \# Secrets passed from .env file  
      AWS\_ACCESS\_KEY\_ID: ${AWS\_ACCESS\_KEY\_ID}  
      AWS\_SECRET\_ACCESS\_KEY: ${AWS\_SECRET\_ACCESS\_KEY}  
    volumes:  
      \- ./kafka-data/connect:/var/lib/kafka/data  
    command:  
      \- bash  
      \- \-c  
      \- |  
        echo "Installing S3 Source Connector..."  
        confluent-hub install \--no-prompt confluentinc/kafka-connect-s3-source:2.5.2  
        /etc/confluent/docker/run

### **B. .env (In root)**

Replace with your actual keys. **Add this file to .gitignore.**

AWS\_ACCESS\_KEY\_ID=AKIA......  
AWS\_SECRET\_ACCESS\_KEY=abc123......

### **C. config/s3\_source.json**

Replace YOUR\_BUCKET\_NAME with the actual S3 bucket name.

{  
  "name": "s3-hybrid-source",  
  "config": {  
    "connector.class": "io.confluent.connect.s3.source.S3SourceConnector",  
    "s3.region": "eu-central-1",  
    "s3.bucket.name": "YOUR\_BUCKET\_NAME\_HERE",  
    "folders": "raw/db,raw/tomtom,raw/owm",  
    "tasks.max": "1",  
    "mode": "GENERIC",  
    "topics.dir": "raw",  
    "topic.regex": ".\*",  
    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",  
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",  
    "confluent.topic.bootstrap.servers": "broker:29092",  
    "confluent.topic.replication.factor": "1",  
    "transforms": "ExtractField",  
    "transforms.ExtractField.type": "org.apache.kafka.connect.transforms.ExtractField$Value",  
    "transforms.ExtractField.field": "text",  
    "scan.interval.ms": "60000"  
  }  
}

### **D. requirements.txt (In root)**

pyspark\>=3.5.0  
duckdb\>=0.9.0  
pandas  
requests  
streamlit

## **🚀 4\. Running the Pipeline**

### **Step 1: Install Python Dependencies**

pip install \-r requirements.txt

### **Step 2: Start Kafka (The Infrastructure)**

Run this command in the project root. It will download the images (approx 2GB) the first time.

docker-compose up \-d

*Wait 60 seconds for the containers to initialize.*

### **Step 3: Configure the Connector (Only needed once per session)**

Tell Kafka to start watching S3:

curl \-X POST \-H "Content-Type: application/json" \--data @config/s3\_source.json http://localhost:8083/connectors

### **Step 4: Run Spark (The Processing)**

This script reads the data flowing from Kafka and prints it to the console (for now).

\# Windows (PowerShell) may need different quoting for packages  
spark-submit \--packages org.apache.spark:spark-sql-kafka-0-10\_2.12:3.5.0 scripts/process\_stream.py

### **Step 5: Shutting Down**

When you are done for the day:

docker-compose stop

(Using stop saves the state. Next time you run up, it resumes from where it left off).

## **🛠 5\. Troubleshooting**

**Error: JAVA\_HOME is not set**

* **Fix:** You installed Python but forgot Java. Install JDK 11 and set your JAVA\_HOME environment variable.

**Error: Docker daemon is not running**

* **Fix:** Open the Docker Desktop application GUI before running the terminal commands.

**Error: AWS\_ACCESS\_KEY\_ID variable is not set**

* **Fix:** Ensure you created the .env file and it is in the same folder as docker-compose.yaml.