# ⚡ Olist E-Commerce Analytics Pipeline — End-to-End ETL with Spark, Airflow & AWS S3

A production-ready data engineering pipeline that ingests raw Olist e-commerce data from **AWS S3**, processes it using **Apache Spark**, orchestrates workflows with **Apache Airflow**, and stores processed analytics-ready data back into S3.

![Data Pipeline](https://img.shields.io/badge/Data-Pipeline-blue)
![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-orange)
![Apache Spark](https://img.shields.io/badge/Apache-Spark-red)
![Docker](https://img.shields.io/badge/Docker-Containers-blue)
![Python](https://img.shields.io/badge/Python-3.8-green)
![AWS S3](https://img.shields.io/badge/AWS-S3-yellow)

---

## 🏗️ System Architecture



![Architecture](app\docs\project-architechture.png)

---

## 📌 Overview

- **Source:** Raw CSV files stored in S3  
- **Processing Engine:** Apache Spark (cluster mode)  
- **Orchestration:** Apache Airflow  
- **Storage:** AWS S3 (raw + processed zones)  
- **Containerization:** Docker & Docker Compose  

---

## 🧰 Tech Stack

| Component       | Technology          |
|----------------|----------------------|
| Workflow       | Apache Airflow       |
| Processing     | Apache Spark         |
| Storage        | AWS S3               |
| Backend        | PostgreSQL (Airflow) |
| Containers     | Docker               |
| Language       | Python 3.8+          |
| File Format    | Parquet              |

---

# 📂 Project Directory Structure

```
olist-E-commerce-etl/
|
├── app/
│   ├── airflow/
│   │   ├── dags/
│   │   │   ├── olist_daily_etl.py
│   │   ├── logs/  
│   │   └── plugins/
│   ├── data/
│   │   ├── olist_customers_dataset.csv
│   │   ├── olist_orderitems_dataset.csv
│   │   ├── olist_orderpayments_dataset.csv
│   │   ├── olist_orderreviews_dataset.csv
│   │   ├── olist_orders_dataset.csv
│   │   ├── olist_products_dataset.csv
│   │   ├── olist_sellers_dataset.csv
│   │   └── product_category_name_translation.csv
│   ├── docs/
│   │   ├── business_questions.md
│   │   ├── project-architechture.png
│   │   ├── setup_guide.md
│   │   └── star_schema.png
│   ├── env
│   ├── env.example
│   ├── spark_etl/
│   │   ├── config/
│   │   │   └── spark_config.yaml
│   │   ├── main/
│   │   │   ├── jobs/
│   │   │   │   ├── business_kpis.py
│   │   │   │   ├── create_dim_date.py
│   │   │   │   ├── main.py
│   │   │   │   └── __init__.py
│   │   │   ├── read/
│   │   │   │   ├── s3_reader.py
│   │   │   │   └── __init__.py
│   │   │   ├── utils/
│   │   │   │   ├── logging_config.py
│   │   │   │   ├── s3_client_object.py
│   │   │   │   ├── snowflake_connector.py
│   │   │   │   ├── spark_session.py
│   │   │   │   └── __init__.py
│   │   │   └── __init__.py
│   │   ├── tests/
│   │   │   ├── upload_data_to_s3.py
│   │   │   └── __init__.py
│   │   └── __init__.py
├── docker/
│   ├── airflow/
│   │   └── Dockerfile
│   └── spark/
│       └── Dockerfile
├── .gitignore
├── docker-compose.yml
├── README.md
├── requirements-airflow.txt
└── requirements.txt
```



---

## 🚀 Setup Instructions

### 1️⃣ Clone Repository
```bash
git clone https://github.com/Subhajit1947/Olist-E-Commerce-Analytics-Pipeline.git
cd Olist-E-Commerce-Analytics-Pipeline
```

### 2️⃣ Create `.env`
```bash
cp .env.example .env
```

Fill AWS keys & S3 paths:
```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
S3_BUCKET=your_bucket
S3_RAW_DIR=raw
S3_PROCESSED_DIR=processed
```

### 3️⃣ Create Airflow Logs Folder
```bash
mkdir app/airflow/logs
```

### 4️⃣ Build Containers
```bash
docker-compose build --no-cache
```

### 5️⃣ Start Services
```bash
docker-compose up -d
```

### 6️⃣ Fix Airflow Spark Connection
In **Airflow Web UI → Admin → Connections → spark_default**

Update host to:
```
spark://spark-master:7077
```

Save.

---

## ▶️ Running the Pipeline

### **Option 1 — Run from Airflow**
- Open DAG: `olist_etl_pipeline`
- Click **Trigger DAG**

### **Option 2 — Run from Docker**
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/app/spark_etl/main/jobs/business_kpis.py
```

---

## 📊 Outputs

- Cleaned dimension tables  
- Fact table with revenue & delivery metrics  
- Parquet outputs stored in S3 processed zone  
- Ready for BI reporting  

---

## 🧪 Troubleshooting

### Restart Airflow
```bash
docker-compose down -v
docker-compose up -d
```

### Test S3 Connectivity
```bash
docker exec spark-master python3 -c "import boto3; print(boto3.client('s3').list_buckets())"
```


## 👨‍💻 Author
Subhajit Das  
Aspiring Data Engineer 

[![GitHub](https://img.shields.io/badge/GitHub-Subhajit1947-black?style=for-the-badge&logo=github)](https://github.com/Subhajit1947)


[![LinkedIn](https://img.shields.io/badge/LinkedIn-Subhajit%20Das-blue?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/subhajit7318/)
  
