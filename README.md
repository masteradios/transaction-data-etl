# Transaction Data Ingestion Pipeline Using Dagster

A production-grade ETL pipeline built with Dagster and Apache Spark, deployed on AWS EC2 for processing and validating merchant transaction data with automated quality reporting.

##  Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Pipeline Workflow](#pipeline-workflow)
- [Database Schema](#database-schema)
- [Key Learnings](#key-learnings)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Screenshots](#screenshots)

##  Overview

This project simulates a real-world scenario where multiple merchants submit their transaction data files for validation and storage. The system automatically ingests files from S3, validates records using Apache Spark, stores valid data in PostgreSQL, and generates quality reports as PDFs.

**Dataset Source**: [Kaggle Transactions Fraud Dataset](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets)

### Sample Data Schema
```
id, date, client_id, card_id, amount, use_chip, merchant_id, 
merchant_city, merchant_state, zip, mcc, errors
```

##  Architecture

<img width="1547" height="637" alt="system_arch1" src="https://github.com/user-attachments/assets/9057fade-21d5-486b-b487-61358cd684ea" />


The pipeline consists of three main jobs orchestrated by Dagster:

1. **Prepare Files for Ingestion** - Discovers new files in S3 and logs them to PostgreSQL
2. **Process Files** - Validates data using Spark and bulk loads valid records
3. **Generate Report** - Creates PDF reports and emails them to merchants

##  Features

### Core Functionality
-  **Concurrent Processing**: Handles up to 5 files simultaneously using PostgreSQL's `FOR UPDATE SKIP LOCKED`
-  **Smart Sensors**: Event-driven architecture with Dagster sensors monitoring file status
-  **Data Validation**: Spark-based validation rules for transaction integrity
-  **Quality Reporting**: Automated PDF generation with validation statistics
-  **Email Notifications**: Automated report delivery to merchants
-  **Bulk Loading**: Optimized PostgreSQL bulk insert using `COPY FROM`

### Production Features
- **Error Handling**: Comprehensive failure hooks and status tracking
- **Observability**: Detailed logging and Dagster UI integration
- **Scalability**: Parallel file processing with configurable concurrency
- **State Management**: Complete audit trail in `JOB_INFO` table

##  Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Dagster |
| Data Processing | Apache Spark (PySpark) |
| Database | PostgreSQL |
| Storage | AWS S3 |
| Report Generation | Java (iText/Apache PDFBox) |
| Email Service | SMTP (Gmail/Outlook) |
| Language | Python 3.10+ |

##  Prerequisites

- Python 3.10
- Apache Spark 3.x
- PostgreSQL 17
- Java 17 (for PDF generation)
- AWS Account (for S3)
- SMTP credentials (for email)

##  Installation

### 1. Clone the Repository
```bash
git clone [Git Repo](https://github.com/masteradios/transaction-data-etl.git)
cd dagster_Test
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Set Up Environment Variables
Create a `.env` file in the root directory:
```env
DB_HOST=hostname
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=postgres

SMTP_SERVER=sender@gmail.com
SMTP_PORT=587
SENDER_EMAIL=receiver@gmail.com
SENDER_PASSWORD=app-password
USE_TLS=true
USE_SSL=false
```

### 5. Set Up Database
```bash
psql -U postgres
CREATE DATABASE transaction_db;
\c transaction_db
```

Run the schema setup:
```sql
-- Job_Info Table
CREATE TABLE IF NOT EXISTS public.job_info (
status VARCHAR(20),
time_updated date,
job_id BIGINT,
file_id VARCHAR(100) 
);
-- Job_Info Sequence to get unique job_id
CREATE SEQUENCE IF NOT EXISTS job_id_sequence
    AS BIGINT
    INCREMENT BY 1
    START WITH 1
    OWNED BY public.job_info.job_id;

-- Link the column to the sequence so it auto-fills
ALTER TABLE public.job_info 
ALTER COLUMN job_id SET DEFAULT nextval('job_id_sequence');

-- transactions Table
CREATE TABLE PUBLIC.transactions (
    id              BIGINT,
    date            TIMESTAMP,
    client_id       VARCHAR,
    card_id         VARCHAR,
    amount          DOUBLE PRECISION,
    use_chip        VARCHAR,
    merchant_id     VARCHAR,
    merchant_city   VARCHAR,
    merchant_state  VARCHAR,
    zip             VARCHAR,
    mcc             VARCHAR,
    errors          VARCHAR,
    reject_reason   VARCHAR
);


-- MCC_Codes Table
CREATE TABLE mcc_codes (
    mcc_code TEXT PRIMARY KEY,
    description TEXT
);

```

### 6. Build PDF Generator JAR
```bash
cd pdfJar 
mvn clean package
```

##  Configuration

### S3 Bucket Structure
```
your-bucket/
├── rawFiles/          # Merchants upload files here
├── inputFiles/        # Files ready for processing
├── processedFiles/    # Archive of processed files
├── validFiles/        # Valid records from Spark
├── rejectFiles/       # Rejected records from Spark
├── summaryFiles/      # Validation summaries
└── reportPdfs/        # Generated reports
```

### Dagster Resources

Configure resources in `definitions.py`:

<img width="1920" height="1030" alt="resources" src="https://github.com/user-attachments/assets/69005ec4-5469-4e1d-9ebe-4a44f700406f" />


##  Pipeline Workflow

### Job 1: Prepare Files for Ingestion

<img width="1920" height="1000" alt="prepare_files_for_ingestion" src="https://github.com/user-attachments/assets/b36d802f-d0d7-4457-b413-faa301fe257e" />


**Sensor**: Checks S3 `rawFiles/` folder every 60 seconds

**Steps**:
1. Fetches new files from S3 `rawFiles/`
2. Moves files to `inputFiles/` and archives to `processedFiles/`
3. Inserts records into `JOB_INFO` table with status `RECEIVED`
4. Validates merchant_id format

**Status Flow**: `RECEIVED`

---

### Job 2: Process Files

<img width="1920" height="1030" alt="processFiles" src="https://github.com/user-attachments/assets/c81d9554-e477-4e98-b675-3d5aa03472f1" />


**Sensor**: Checks for files with status `RECEIVED` every 30 seconds

**Concurrency**: Maximum 5 concurrent runs using PostgreSQL `FOR UPDATE SKIP LOCKED`

**Steps**:
1. **Update Status**: Marks file as `PROCESSING`
2. **Spark Submit**: Runs validation script
   - Validates transaction records
   - Creates valid records file
   - Creates reject records file
   - Generates summary statistics
3. **Bulk Load**: Inserts valid records into `TRANSACTIONS` table using `COPY FROM`
4. **Update Status**: Marks as `PROCESSED` on success, `FAILED DUE TO ERROR` on failure

**Spark Validation Rules**:
- Amount validation
- Merchant state validation
- ZIP code validation
- Chip usage validation

**Output Files**:
| File Type | Location | Contents |
|-----------|----------|----------|
| Valid Records | `s3://bucket/validFiles/{file_id}/` | Records passing validation |
| Reject Records | `s3://bucket/rejectFiles/{file_id}/` | Records failing validation |
| Summary | `s3://bucket/summaryFiles/{file_id}/` | Validation statistics |

---

### Job 3: Generate Report

<img width="1920" height="1026" alt="generateReport" src="https://github.com/user-attachments/assets/08329dfd-b207-429b-be73-2574cc7959b5" />


**Sensor**: Checks for files with status `PROCESSED` every 10 seconds

**Steps**:
1. **Update Status**: Marks file as `GENERATING REPORT`
2. **Fetch Merchant Info**: Retrieves merchant details from `MERCHANT_INFO`
3. **Generate PDF**: Creates data quality report with:
   - Total records processed
   - Valid vs rejected counts
   - Breakdown by validation rule
   - Merchant information
4. **Send Email**: Emails PDF report to merchant
5. **Update Status**: Marks as `INGESTION COMPLETE`

**Report Contents**:
[86438_10.pdf](https://github.com/user-attachments/files/24530854/86438_10.pdf)


##  Database Schema

### JOB_INFO Table
| Column | Type | Description |
|--------|------|-------------|
| JOB_ID | SERIAL | Primary key, auto-generated |
| FILE_ID | VARCHAR | Unique file identifier |
| STATUS | VARCHAR | Current processing status |
| TIME_UPDATED | DATE | Last update timestamp |

**Status Values**:
- `RECEIVED` → `PROCESSING` → `PROCESSED` → `GENERATING REPORT` → `INGESTION COMPLETE`
- `FAILED DUE TO ERROR` (on failure)

  
<img width="643" height="632" alt="job_info_pg" src="https://github.com/user-attachments/assets/a6e5ec1d-05c7-4ade-846d-77b20d8c103c" />


### MERCHANT_INFO Table

<img width="702" height="652" alt="merchant_info" src="https://github.com/user-attachments/assets/c23b28c2-d924-4fd4-b573-3f1db93fa7ec" />

### TRANSACTIONS Table
Schema matches the Kaggle dataset with all transaction fields.

<img width="1626" height="727" alt="transactions_pg" src="https://github.com/user-attachments/assets/c213567a-0708-4a4a-93b1-f4c1309b1526" />

#### Total Transactions Count 

<img width="287" height="107" alt="total_transactions" src="https://github.com/user-attachments/assets/2dfd8625-a4f3-4257-a0c3-d74bc6bf3716" />


### MCC_Codes Table

<img width="517" height="743" alt="mcc_codes_pg" src="https://github.com/user-attachments/assets/53ff3232-30b1-474a-bfe6-451a8ef8b845" />


##  Key Learnings

### 1. Database Concurrency with `FOR UPDATE SKIP LOCKED`
```sql
SELECT * FROM JOB_INFO 
WHERE STATUS = 'RECEIVED'
ORDER BY JOB_ID DESC
LIMIT 5
FOR UPDATE SKIP LOCKED
```
- Enables safe concurrent processing
- Prevents race conditions
- Allows multiple workers to process different files simultaneously

### 2. PostgreSQL Sequences
```sql
CREATE SEQUENCE IF NOT EXISTS job_id_sequence
    AS BIGINT
    INCREMENT BY 1
    START WITH 1
    OWNED BY public.job_info.job_id;
```
- Generates unique job IDs automatically
- Thread-safe and efficient

### 3. Bulk Loading with COPY FROM
```python
with open(latest_file, "r") as f:
    cur.copy_expert(
            """
                        COPY transactions (
                            id, date, client_id, card_id, amount, use_chip,
                            merchant_id, merchant_city, merchant_state, zip, mcc, errors,reject_reason
                        )
                        FROM STDIN
                        WITH (
                            FORMAT CSV,
                            HEADER TRUE,
                            QUOTE '"',
                            ESCAPE '"'
                        )
            """,
                        f
                    )
```
- Optimized for large datasets

### 4. Dagster Hooks
- Success hooks for status updates
  ```python
  @success_hook(required_resource_keys={"postgresConn"})
  def updatePostgresOnSuccess(context):
  ```
  
- Failure hooks for error handling
  ```python
  @failure_hook(required_resource_keys={"postgresConn"})
  def updatePostgresOnFailure(context):
  ```

### 5. Event-Driven Architecture
- Sensors for file discovery
- Status-based workflow transitions
- Decoupled job execution

##  Project Structure
```
transaction-ingestion-pipeline/
├── dagster_Test/
│   ├── jobs/
│   │   ├── Job_PrepareFiles.py
│   │   ├── Job_ProcessFiles.py
│   │   └── Job_GenerateReport.py
│   ├── resources/
│   │   ├── S3BucketManager.py
│   │   ├── PostgresManager.py
│   │   ├── SparkResource.py
│   │   └── EmailResourceManager.py
│   ├── configs/
│   │   ├── IngestConfig.py
│   │   └── MerchantConfig.py
│   ├── spark_scripts/
│   │   └── validation_script.py
│   └── definitions.py
├── pdfJar/
│   └── generateReportPdf/
├── .env
├── requirements.txt
└── README.md
```

<img width="1920" height="622" alt="s3" src="https://github.com/user-attachments/assets/29ba829d-db03-45b4-98be-1b23c480dace" />



##  Usage

### Start Dagster
```bash
# Set environment variables
export DAGSTER_HOME=/path/to/dagster/home

# Start Dagster daemon (for sensors)
dagster-daemon run

# Start Dagster UI
dagster dev
```

Access the UI at `http://localhost:3000`

![Dagster UI](./docs/dagster_ui.png)

### Enable Sensors

1. Navigate to Automation → Sensors
2. Enable the following sensors:
   - `prepare_files_schedule`
   - `sensorTofetchFileToProcess`
   - `sensorTofetchFileToGenerateReport`

   <img width="1920" height="1021" alt="sensor_s3" src="https://github.com/user-attachments/assets/50fe98c7-da5f-4ddb-9356-7afca2d0379e" />

   <img width="1920" height="1023" alt="sensor_fetch_files_to_process" src="https://github.com/user-attachments/assets/2af04ef9-0dd8-4510-bd66-b89a7a321569" />

   <img width="1920" height="1030" alt="sensor_fetch_files_to_create_pdf" src="https://github.com/user-attachments/assets/7644e1b4-709d-49b3-bc0b-794e0f5abba9" />

<img width="1920" height="1022" alt="dagster_jobs" src="https://github.com/user-attachments/assets/ce327408-da13-4293-a02b-c0661df299f3" />

### Upload Files to S3
```bash
aws s3 cp your_file.csv s3://your-bucket/rawFiles/merchant_id_sequence.csv
```

File naming convention: `{merchant_id}_{sequence_number}.csv`
Example: `20519_1.csv`

##  Screenshots

### Pipeline Overview

<img width="1547" height="637" alt="system_arch1" src="https://github.com/user-attachments/assets/78eabb7d-9226-45dc-9e68-ddefdeca5f15" />


### Job Execution
<img width="1920" height="876" alt="generateReport_Run" src="https://github.com/user-attachments/assets/bb9063c2-2c91-4b72-9218-5967d9ecda5b" />

<img width="1920" height="1027" alt="processFiles_Run" src="https://github.com/user-attachments/assets/e7de7634-6c4d-4d80-bceb-26d5d6b01a8b" />

### Database Status Tracking
<img width="1626" height="727" alt="transactions_pg" src="https://github.com/user-attachments/assets/d6b52712-bfcc-4542-9f2e-a626cf7dbae4" />

### Generated PDF Report

![PDF Report][75936_4.pdf](https://github.com/user-attachments/files/24531012/75936_4.pdf)

### Email Notification
<img width="1531" height="692" alt="email" src="https://github.com/user-attachments/assets/0528bbbc-a987-4dd0-bc27-96a7f9176cd2" />


##  Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

##  Author

Aditya Kushwaha
- GitHub: [@masteradios](https://github.com/masteradios)
- LinkedIn: [LinkedIn](https://www.linkedin.com/in/aditya-kushwaha-264467269/)

##  Acknowledgments

- Dataset provided by [Kaggle](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets)
- Inspired by real-world data ingestion challenges
- Built as a learning project for Dagster and ETL best practices

---

 If you found this project helpful, please consider giving it a star!
