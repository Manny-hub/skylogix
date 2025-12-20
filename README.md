# 🌦️ Skylogix — Weather Data Engineering Pipeline

Skylogix is an end-to-end **data engineering ETL pipeline** that ingests weather data from the **OpenWeather API**, stages raw data in **MongoDB**, transforms and normalizes it, and loads analytics-ready data into **PostgreSQL** using **Apache Airflow** for orchestration.

This project demonstrates real-world data engineering concepts including API ingestion, idempotent upserts, schema design, orchestration, and analytics enablement.

---

## 📌 Project Objectives

* Collect real-time weather data for multiple African cities
* Store **raw, immutable data** for traceability
* Transform and normalize data for analytics use cases
* Load data into a relational warehouse
* Schedule and orchestrate the pipeline using Airflow
* Enable downstream analytics and integration with logistics data


![alt text](<OpenWeather ETL Pipeline.png>)
---

## 🏗️ Architecture Overview

```
OpenWeather API
      ↓
Python Ingestion Script
      ↓
MongoDB (weather_raw)
      ↓
Apache Airflow
      ↓
Transform & Normalize
      ↓
PostgreSQL (weather_readings)
      ↓
Analytics / Dashboards
```

---

## 🛠️ Technology Stack

* **Python 3.10+**
* **OpenWeather API**
* **MongoDB** (Raw / Staging Layer)
* **PostgreSQL** (Analytics Warehouse)
* **Apache Airflow**
* **Pandas**
* **SQLAlchemy**
* **Docker (optional)**

---

## 📁 Repository Structure

```
skylogix/
├── dags/
│   ├── weather_etl.py          # Airflow DAG
│   └── utils.py                # Task callables
│
├── extract_staging.py      # API → MongoDB ingestion
├── stage_transform.py      # MongoDB → normalized dataset
├── load.py                 # Load into PostgreSQL
├── .env.example
├── requirements.txt
└── README.md

```

---

## 🧪 Data Flow Description

### 1️⃣ Data Ingestion (Extract)

* Fetches weather data from the OpenWeather API
* Generates a deterministic `_id` for each record
* Performs **upsert** into MongoDB (`weather_raw`)
* Ensures idempotency and safe retries

### 2️⃣ Data Transformation

* Reads raw weather documents from MongoDB
* Flattens nested JSON structures
* Normalizes fields for relational storage
* Saves transformed data as a pickle file (Airflow XCom-safe)

### 3️⃣ Data Loading

* Reads transformed data from pickle
* Loads into PostgreSQL table `weather_readings`
* Optimized for time-series analytics

---

## 🗄️ Database Design

### MongoDB (Staging)

* **Database:** `weather_raw`
* **Collection:** `weather_raw`
* **Indexes:**

  * `_id` (unique)
  * `city`
  * `updatedAt`

---

### PostgreSQL (Warehouse)

Table: `weather_readings`

Key fields include:

* City & country
* Coordinates
* Temperature, humidity, pressure
* Wind speed and direction
* Rain & snow volume
* Observation timestamp

DDL is available in:

```
sql/weather_readings.sql
```

---

## 📊 Analytics Use Cases

### Weather Trends Per City

```sql
SELECT
  city,
  DATE(observed_at) AS day,
  AVG(temp_c) AS avg_temp
FROM weather_readings
GROUP BY city, day;
```

### Extreme Weather Detection

```sql
SELECT *
FROM weather_readings
WHERE wind_speed_ms > 15
   OR rain_1h_mm > 20;
```

### Logistics Integration (Conceptual)

Weather data can be joined with logistics or delivery datasets using city and timestamp to analyze delays caused by weather conditions.

---

## ⏱️ Airflow DAG

* **DAG Name:** `daily_weather_etl`
* **Schedule:** Daily at 06:00 WAT
* **Tasks:**

  * `extract_weather_data`
  * `transform_weather_data`
  * `load_weather_data`

The DAG ensures proper sequencing, retries, and observability.

---

## ⚙️ Configuration

Create a `.env` file using `.env.example`:

```env
API_KEY=your_openweather_api_key

MONGO_URI=mongodb://user:password@localhost:27017/weather_raw
MONGO_DB=weather_raw
MONGO_COLLECTION=weather_raw

POSTGRES_STRING=postgresql+psycopg2://user:password@localhost:5432/weather_dw

CITIES=Nairobi,KE,Lagos,NG,Accra,GH,Johannesburg,ZA
```

---

## ▶️ Running the Project

### 1️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

### 2️⃣ Start databases

Ensure MongoDB and PostgreSQL are running locally or via Docker.

### 3️⃣ Start Airflow

```bash
airflow db init
airflow scheduler
airflow webserver
```

### 4️⃣ Trigger the DAG

* Open Airflow UI
* Enable `daily_weather_etl`
* Trigger manually or wait for schedule

---

## 🔐 Assumptions & Design Choices

* Raw data is immutable once ingested
* MongoDB is used strictly as a staging layer
* PostgreSQL is optimized for analytics workloads
* Airflow handles orchestration, retries, and scheduling

---

## 📦 Final Deliverables

* Python ingestion script (API → MongoDB)
* Transformation & loading scripts
* Airflow DAG
* MongoDB and PostgreSQL schemas
* Sample analytics queries
* Architecture diagram
* This README

---

## 👤 Author

**Ayomide Emmanuel Adeyeye**
Data Engineering & Backend Development
AltSchool Africa


