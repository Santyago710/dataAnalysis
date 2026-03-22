# 📊 Public Sentiment Analysis on Political Discourse in Colombia

## 📌 Project Topic
This project focuses on the structured analysis of political
and social discourse in Colombia through the implementation
of a data pipeline designed to handle textual information.
The scope includes the ingestion of heterogeneous textual
data, its storage in raw formats, and its transformation into
structured datasets suitable for analysis. A key component
within this scope is the preprocessing stage, where textual data
is cleaned, normalized, and prepared for Natural Language
Processing (NLP) tasks. This includes handling inconsistencies
such as missing values, encoding issues, and noise commonly
found in opinion-based content. Following this, the project
applies sentiment analysis techniques to classify and quantify
the polarity of opinions expressed in the data. The project
also includes the generation of aggregated insights, such as
sentiment trends over time and comparative patterns across
different types of discourse. These outputs are intended to
support interpretation and will serve as the foundation for
future visualization components

---

## 👥 Team Members

- Ammi Susana Pineda Guzman 
- David Santiago Buitrago Cerquera
- Cristhian Samuel Truque Cabrejo

---

## 🌐 Data Sources

### 🔹 API Source
- Reddit API  
- Subreddits: r/colombia, r/politics  
- Data collected: posts, comments, metadata  

### 🔹 Web Scraping Source
- La Silla Vacía  
- Data collected: article title, content, publication date, author  

---

## 📂 Repository Structure
- API/ # Reddit data collection API
- Webscrapping/ # La silla vacia data collection webscrapping
- datalake_bronze/ # Raw JSON data (API and scraping)
- datalake_silver/ # Processed data (Parquet format)
- datalake_gold/ # Aggregated data for analysis
- airflow/dags/ # Airflow DAGs for orchestration
- dashboard/ # Visualization dashboards (Plotly Dash)
- notebooks/ # Exploratory data analysis
- workshop_1/ # Workshop 1 deliverables (PDF + data samples)

## ⚠️ Notes

- Data is collected from publicly available sources  
- The project focuses on Spanish-language data  
- The pipeline will be developed progressively in future workshops 
