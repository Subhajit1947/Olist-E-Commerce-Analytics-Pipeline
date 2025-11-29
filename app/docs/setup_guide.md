# 🚀 Project Setup Instructions

Follow these steps to set up and run the project.

---

## 📥 1. Clone the Repository
First, clone the repo:

```bash
git clone https://github.com/Subhajit1947/Olist-E-Commerce-Analytics-Pipeline.git
cd Olist-E-Commerce-Analytics-Pipeline
```

---

## ⚙️ 2. Requirements
Make sure the following are installed:

- Docker Desktop  
- AWS account (for credentials)

---

## 🔐 3. Create `.env` File
Inside the **app** directory:

1. Create a file named `.env`  
2. Copy the content from `.env.example`  
3. Add all the necessary keys  

```txt
app/
 ├── .env
 └── .env.example
```

---

## 📂 4. Airflow Logs Directory
Inside the **airflow** folder, create:

```bash
mkdir airflow/logs
```

---

## 🛠️ 5. Build Docker Containers
Run the following command:

```bash
docker-compose build --no-cache
```

---

## ▶️ 6. Start All Services

```bash
docker-compose up -d
```

---

## 📝 7. Update Airflow Connection

1. Go to **Airflow Webserver**
2. Open **Admin → Connections**
3. Select **spark_default**
4. Change the host to:

```
spark://spark-master:7077
```

5. Save it.

---

## 📊 8. Running Business Questions

### **Option A — Run via Airflow**

If you want to run business questions through Airflow:

- Uncomment the **business question** section  
- Uncomment the **business question dependency** section  

Then trigger the DAG from the Airflow UI.

---

### **Option B — Run via Docker (Spark Submit)**

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/app/spark_etl/main/jobs/business_kpis.py
```

---

You are now ready to proceed. 🚀
