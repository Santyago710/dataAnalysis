# 📊 Public Sentiment Analysis on Political Discourse in Colombia

## 📌 Project Topic
This project analyzes political and social discourse in Colombia through a Medallion Architecture data pipeline (Bronze → Silver → Gold), applying sentiment analysis and NLP techniques to data from Reddit and La Silla Vacía. Results are visualized through two interactive Plotly Dash dashboards.

---

## 👥 Team Members
- Ammi Susana Pineda Guzman
- David Santiago Buitrago Cerquera
- Cristhian Samuel Truque Cabrejo

---

## 🌐 Data Sources
### 🔹 API Source
- Reddit API — Subreddit: r/Colombia
- Data collected: posts, metadata, engagement scores

### 🔹 Web Scraping Source
- La Silla Vacía (Opinion Section)
- Data collected: article title, content, author, publication date, tags

---
```
## 📂 Repository Structure
dataAnalysis/
├── workshop1/                  # Workshop 1 — Project Definition
│   ├── API/                    # Reddit API script
│   ├── Webscrapping/           # La Silla Vacía scraper
│   ├── notebook/               # Exploratory notebooks
│   └── Political_y_social_discourse_sentimient_analysis.pdf
├── workshop2/                  # Workshop 2 — Bronze & Silver Pipeline
│   ├── airflow/dags/           # Bronze + Silver DAGs
│   ├── datalake_bronze/        # Raw JSON files
│   ├── datalake_silver/        # Processed Parquet files
│   ├── datalake_gold/          # Gold Parquet files
│   ├── notebooks/              # Silver exploration notebooks
│   └── docker-compose.yml
├── workshop3/                  # Workshop 3 — Gold Layer (PySpark)
│   ├── airflow/dags/           # Gold DAG with PySpark
│   ├── datalake_bronze/
│   ├── datalake_silver/
│   ├── datalake_gold/          # Governance + Storytelling Parquets
│   ├── notebooks/              # Data quality + Gold exploration
│   └── docker-compose.yml
├── workshop4/                  # Workshop 4 — Dashboards (current)
│   ├── airflow/dags/           # All DAGs (Bronze, Silver, Gold)
│   ├── dashboard/              # Plotly Dash applications
│   │   ├── governance_app.py   # Governance dashboard (port 8050)
│   │   └── storytelling_app.py # Storytelling dashboard (port 8051)
│   ├── datalake_bronze/
│   ├── datalake_silver/
│   ├── datalake_gold/
│   ├── notebooks/
│   └── docker-compose.yml
└── README.md
```
---

## 🏗️ Pipeline Architecture
```
Reddit API ──┐                              ┌── governance.parquet
├── Bronze (JSON) ── Silver ───┤
La Silla ───┘    (Airflow DAG)  (Pandas)   └── storytelling_*.parquet
(PySpark Gold DAG)
│
┌───────────┴───────────┐
Governance              Storytelling
Dashboard               Dashboard
(port 8050)             (port 8051)
```
---

## 🚀 How to Run Each Workshop

### Prerequisites (all workshops)
- Docker Compose v2.24+
- Python 3.11+ with conda
- Java 17 (installed inside Docker automatically)

---

### 📘 Workshop 1 — Project Definition & Data Exploration
No Docker required. Run scripts locally:

```bash
cd workshop1

# Install dependencies
pip install requests beautifulsoup4 pandas jupyter

# Run Reddit API extraction
python API/reddit_api.py

# Run La Silla Vacía scraper
python Webscrapping/lasillavacia_scraper.py

# Open exploratory notebook
jupyter notebook notebook/
```

---

### 📗 Workshop 2 — Bronze & Silver Pipeline

```bash
cd workshop2

# Set permissions
sudo chmod -R 777 datalake_bronze datalake_silver datalake_gold airflow

# Start Docker
/usr/local/bin/docker-compose up -d
# Wait 5 minutes for dependencies

# Initialize Airflow (first time only)
/usr/local/bin/docker-compose run --rm airflow-webserver airflow db migrate
/usr/local/bin/docker-compose run --rm airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com

# Create fs_default connection in localhost:8080
# Admin → Connections → Add:
#   Connection Id: fs_default
#   Connection Type: File (path)
#   Path: /opt/airflow

# Trigger DAGs in Airflow UI (localhost:8080)
# 1. bronze_ingestion
# 2. silver_processing
```

# Open exploratory notebook
jupyter notebook notebook/

---

### 📙 Workshop 3 — Gold Layer with PySpark

```bash
cd workshop3

# Set permissions
sudo chmod -R 777 datalake_bronze datalake_silver datalake_gold airflow

# Start Docker
/usr/local/bin/docker-compose up -d
# Wait 5 minutes for PySpark to install

# Initialize Airflow (first time only)
/usr/local/bin/docker-compose run --rm airflow-webserver airflow db migrate
/usr/local/bin/docker-compose run --rm airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com

# Install Java (required every restart)
docker exec -it --user root workshop3-airflow-scheduler-1 bash -c "apt-get update && apt-get install -y default-jdk"
docker exec -it --user root workshop3-airflow-webserver-1 bash -c "apt-get update && apt-get install -y default-jdk"

# Create fs_default connection in localhost:8080

# Trigger DAGs in order:
# 1. bronze_ingestion
# 2. silver_processing
# 3. gold_processing

# Explore results
jupyter notebook notebooks/
```

---

### 📕 Workshop 4 — Interactive Dashboards

```bash
cd workshop4

# Set permissions
sudo chmod -R 777 datalake_bronze datalake_silver datalake_gold airflow

# Start Docker
/usr/local/bin/docker-compose up -d
# Wait 5 minutes for PySpark to install

# Initialize Airflow (first time only)
/usr/local/bin/docker-compose run --rm airflow-webserver airflow db migrate
/usr/local/bin/docker-compose run --rm airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com

# Install Java (required every restart)
docker exec -it --user root workshop4-airflow-scheduler-1 bash -c "apt-get update && apt-get install -y default-jdk"
docker exec -it --user root workshop4-airflow-webserver-1 bash -c "apt-get update && apt-get install -y default-jdk"

# Create fs_default connection in localhost:8080

# Trigger DAGs in order:
# 1. bronze_ingestion
# 2. silver_processing
# 3. gold_processing

# Install dashboard dependencies
pip install dash plotly dash-bootstrap-components pandas pyarrow

# Launch Governance Dashboard
python dashboard/governance_app.py
# Open: http://localhost:8050

# Launch Storytelling Dashboard (new terminal)
python dashboard/storytelling_app.py
# Open: http://localhost:8051
```

---

## 📦 DAGs
| DAG | Schedule | Description |
|-----|----------|-------------|
| `bronze_ingestion` | `@daily` | Extracts raw data from Reddit API and La Silla Vacía |
| `silver_processing` | Manual (FileSensor) | Cleans Bronze JSON → Parquet with NLP preprocessing |
| `gold_processing` | `@weekly` | PySpark aggregations + sentiment analysis → Gold Parquet |

---

## 📊 Dashboards (Workshop 4)
| Dashboard | Port | Audience | Data Source |
|-----------|------|----------|-------------|
| Governance | 8050 | Technical | `governance_*.parquet` |
| Storytelling | 8051 | Functional user | `storytelling_*.parquet` |

---

## ⚠️ Notes
- Data is collected from publicly available sources
- The project focuses on Spanish-language political discourse
- Sentiment analysis uses a domain-specific Spanish lexicon
- Java must be reinstalled after every Docker restart (PySpark dependency)
- Each workshop folder is independent with its own Docker environment
