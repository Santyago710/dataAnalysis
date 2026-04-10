"""
🗂️ SILVER PROCESSING DAG - Complete Data Preprocessing Pipeline

Detects new JSON files in datalake_bronze/, applies robust preprocessing with Pandas,
and saves output as Parquet with full traceability to source files.

WORKFLOW:
1. FileSensor detects new reddit_*.json and lasillavacia_*.json
2. Preprocessing tasks apply cleaning, deduplication, outlier detection, text cleaning
3. Saves as Parquet in datalake_silver/ with metadata and traceability
4. Validation task ensures output integrity
"""

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import re
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import List, Dict, Tuple

# ============================================================================
# CONFIGURATION
# ============================================================================
BRONZE_PATH = Path("/opt/airflow/datalake_bronze")
SILVER_PATH = Path("/opt/airflow/datalake_silver")

# Spanish stopwords for text cleaning (47 common words)
STOPWORDS_ES = {
    "de", "la", "el", "en", "y", "a", "los", "las", "un", "una",
    "es", "se", "no", "con", "por", "que", "del", "al", "lo", "su",
    "para", "como", "más", "pero", "sus", "le", "ya", "o", "fue",
    "este", "ha", "si", "porque", "esta", "son", "entre", "cuando",
    "muy", "sin", "sobre", "también", "me", "hasta", "hay", "donde",
    "quien", "desde", "todo", "nos", "durante", "uno", "ni", "contra",
    "ese", "esta", "esto", "eso", "esa", "esos", "esas", "estás", "está",
    "están", "estamos", "estáis", "estaba", "estabas", "estábamos", "estabais",
    "estaban", "estads", "estado", "estados", "estada", "estadas"
}

logger = logging.getLogger(__name__)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def clean_text(text: str) -> str:
    """Clean text: remove URLs, special characters, and stopwords."""
    if not isinstance(text, str):
        return ""
    text = text.lower()
    # Remove URLs
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)
    # Remove special characters but preserve ñ and accents
    text = re.sub(r"[^\w\sáéíóúüñ]", " ", text)
    # Normalize spaces
    text = re.sub(r"\s+", " ", text).strip()
    # Tokenize and filter stopwords
    tokens = [w for w in text.split() if w not in STOPWORDS_ES and len(w) > 2]
    return " ".join(tokens)

def detect_outliers_iqr(series: pd.Series, multiplier: float = 1.5) -> pd.Series:
    """Detect outliers using Interquartile Range (IQR) method."""
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    return (series >= lower_bound) & (series <= upper_bound)

def detect_outliers_zscore(series: pd.Series, threshold: float = 3.0) -> pd.Series:
    """Detect outliers using Z-Score method."""
    z_scores = np.abs((series - series.mean()) / series.std())
    return z_scores <= threshold

def validate_parquet_file(file_path: Path) -> bool:
    """Validate that a Parquet file is readable and valid."""
    try:
        df = pd.read_parquet(file_path)
        logger.info(f"✅ Valid Parquet: {file_path.name} ({len(df)} rows)")
        return True
    except Exception as e:
        logger.error(f"❌ Error validating Parquet {file_path.name}: {e}")
        return False

def get_new_files(source_pattern: str) -> List[Path]:
    """Get list of files matching the given glob pattern."""
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)
    files = sorted(BRONZE_PATH.glob(source_pattern))
    return files

# ============================================================================
# SOURCE-SPECIFIC PREPROCESSING FUNCTIONS
# ============================================================================

def preprocess_reddit_data(files: List[Path]) -> Tuple[pd.DataFrame, list]:
    """
    Preprocess Reddit data with:
    - Deduplication by URL
    - Type conversion (date, score)
    - Null handling
    - Remove records without title
    - Outlier detection in score
    - NLP text cleaning
    """
    logger.info(f"📥 Processing {len(files)} Reddit files...")
    all_records = []
    processed_files = []
    
    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                if isinstance(data, list):
                    for record in data:
                        record["source_file"] = f.name
                        record["source_path"] = str(f)
                    all_records.extend(data)
                    processed_files.append(f.name)
                    logger.info(f"✓ {f.name}: {len(data)} records")
        except Exception as e:
            logger.error(f"⚠️ Error reading {f.name}: {e}")
            continue
    
    if not all_records:
        logger.warning("⚠️ No Reddit data to process")
        return pd.DataFrame(), processed_files
    
    df = pd.DataFrame(all_records)
    logger.info(f"📊 Raw Reddit: {len(df)} records")
    
    # ========== 1. DEDUPLICATION ==========
    initial_count = len(df)
    df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)
    logger.info(f"🔄 Deduplication: {initial_count} → {len(df)} (-{initial_count - len(df)})")
    
    # ========== 2. TYPE CONVERSION ==========
    # Unix timestamp to datetime
    df["date"] = pd.to_numeric(df["date"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], unit="s", errors="coerce")
    # Score to integer
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype(int)
    logger.info("✓ Data types converted")
    
    # ========== 3. NULL HANDLING ==========
    df["title"] = df["title"].fillna("").str.strip()
    df["text"] = df["text"].fillna("")
    df["author"] = df["author"].fillna("unknown")
    df["url"] = df["url"].fillna("")
    df["source_file"] = df["source_file"].fillna("")
    logger.info(f"✓ Nulls handled")
    
    # ========== 4. MINIMUM VALIDATION ==========
    initial_count = len(df)
    df = df[df["title"].str.len() > 3].reset_index(drop=True)
    logger.info(f"📝 Valid titles: {initial_count} → {len(df)} (-{initial_count - len(df)})")
    
    # ========== 5. OUTLIER DETECTION IN SCORE ==========
    if len(df) > 0 and df["score"].std() > 0:
        initial_count = len(df)
        outlier_mask = detect_outliers_iqr(df["score"], multiplier=1.5)
        df = df[outlier_mask].reset_index(drop=True)
        logger.info(f"🎯 Outliers removed: {initial_count} → {len(df)} (-{initial_count - len(df)})")
    
    # ========== 6. NLP TEXT CLEANING ==========
    df["title_clean"] = df["title"].apply(clean_text)
    df["text_clean"] = df["text"].apply(clean_text)
    df["text_length"] = df["text"].str.len()
    logger.info("✓ Text cleaned")
    
    # ========== 7. FINAL SCHEMA ==========
    df = df[[
        "title", "title_clean", "text", "text_clean", "text_length",
        "author", "date", "score", "url", "source_file", "source_path"
    ]]
    
    logger.info(f"✅ Reddit preprocessed: {len(df)} final records")
    return df, processed_files

def preprocess_lasillavacia_data(files: List[Path]) -> Tuple[pd.DataFrame, list]:
    """
    Preprocess La Silla Vacía data with:
    - Deduplication by URL
    - Date conversion (ISO 8601 to datetime)
    - Null handling
    - Remove records without title
    - NLP text cleaning
    - Field normalization
    """
    logger.info(f"📥 Processing {len(files)} La Silla Vacía files...")
    all_records = []
    processed_files = []
    
    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                if isinstance(data, list):
                    for record in data:
                        record["source_file"] = f.name
                        record["source_path"] = str(f)
                    all_records.extend(data)
                    processed_files.append(f.name)
                    logger.info(f"✓ {f.name}: {len(data)} records")
        except Exception as e:
            logger.error(f"⚠️ Error reading {f.name}: {e}")
            continue
    
    if not all_records:
        logger.warning("⚠️ No La Silla Vacía data to process")
        return pd.DataFrame(), processed_files
    
    df = pd.DataFrame(all_records)
    logger.info(f"📊 Raw La Silla Vacía: {len(df)} records")
    
    # ========== 1. DEDUPLICATION ==========
    initial_count = len(df)
    df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)
    logger.info(f"🔄 Deduplication: {initial_count} → {len(df)} (-{initial_count - len(df)})")
    
    # ========== 2. DATE CONVERSION ==========
    # Convert ISO 8601 date to datetime
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce", utc=True)
    df["fecha"] = df["fecha"].dt.tz_localize(None)  # Remove timezone info
    logger.info("✓ Dates converted")
    
    # ========== 3. NULL HANDLING ==========
    df["titulo"] = df["titulo"].fillna("").str.strip()
    df["autor"] = df["autor"].fillna("Unknown")
    df["extracto"] = df["extracto"].fillna("")
    df["contenido"] = df["contenido"].fillna("")
    df["etiquetas"] = df["etiquetas"].fillna("")
    df["url"] = df["url"].fillna("")
    df["source_file"] = df["source_file"].fillna("")
    logger.info("✓ Nulls handled")
    
    # ========== 4. MINIMUM VALIDATION ==========
    initial_count = len(df)
    df = df[df["titulo"].str.len() > 3].reset_index(drop=True)
    logger.info(f"📝 Valid titles: {initial_count} → {len(df)} (-{initial_count - len(df)})")
    
    # ========== 5. NLP TEXT CLEANING ==========
    df["titulo_clean"] = df["titulo"].apply(clean_text)
    df["extracto_clean"] = df["extracto"].apply(clean_text)
    df["contenido_clean"] = df["contenido"].apply(clean_text)
    df["contenido_length"] = df["contenido"].str.len()
    logger.info("✓ Text cleaned")
    
    # ========== 6. FINAL SCHEMA ==========
    df = df[[
        "titulo", "titulo_clean", "autor", "fecha",
        "extracto", "extracto_clean", "contenido", "contenido_clean",
        "contenido_length", "etiquetas", "url", "source_file", "source_path"
    ]]
    
    logger.info(f"✅ La Silla Vacía preprocessed: {len(df)} final records")
    return df, processed_files



# ============================================================================
# DAG DEFINITION
# ============================================================================

@dag(
    dag_id="silver_processing",
    description="Silver Layer Processing with FileSensor, Pandas preprocessing, and Parquet output",
    schedule_interval=None,  # Triggered by FileSensor
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["silver", "processing", "etl"],
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }
)
def silver_processing_dag():
    """
    Silver Layer processing pipeline:
    1. Detects new JSON in datalake_bronze/ with FileSensor
    2. Preprocesses data (nulls, types, outliers, text)
    3. Saves as Parquet with traceability to source files
    4. Validates quality of generated Parquet
    """
    
    # ======================== TASK 1: DETECT NEW REDDIT FILES ========================
    wait_for_reddit = FileSensor(
        task_id="detect_reddit_files",
        filepath=str(BRONZE_PATH / "reddit_*.json"),
        poke_interval=30,
        timeout=3600,
        mode="poke",
        fs_conn_id="fs_default",
        doc="Waits for arrival of new Reddit JSON files in datalake_bronze/"
    )
    
    # ======================== TASK 2: DETECT NEW LA SILLA VACÍA FILES ========================
    wait_for_lsv = FileSensor(
        task_id="detect_lasillavacia_files",
        filepath=str(BRONZE_PATH / "lasillavacia_*.json"),
        poke_interval=30,
        timeout=3600,
        mode="poke",
        fs_conn_id="fs_default",
        doc="Waits for arrival of new La Silla Vacía JSON files in datalake_bronze/"
    )
    
    # ======================== TASK 3: PROCESS REDDIT ========================
    @task(
        task_id="process_reddit",
        doc="Preprocess Reddit data with cleaning, deduplication and validation"
    )
    def process_reddit_task():
        """Get Reddit files, preprocess and save as Parquet."""
        SILVER_PATH.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Search for files
        reddit_files = get_new_files("reddit_*.json")
        if not reddit_files:
            logger.warning("⚠️ No Reddit files to process")
            return {"status": "skipped", "records": 0, "output_file": None}
        
        # Preprocess
        df, source_files = preprocess_reddit_data(reddit_files)
        
        if df.empty:
            logger.warning("⚠️ Empty DataFrame after preprocessing")
            return {"status": "empty", "records": 0, "output_file": None}
        
        # Save as Parquet with traceability metadata
        output_file = SILVER_PATH / f"reddit_{timestamp}.parquet"
        
        # Add additional metadata
        df.attrs = {
            "processed_timestamp": timestamp,
            "source_files": ",".join(source_files),
            "record_count": len(df),
            "processor": "silver_processing_dag"
        }
        
        df.to_parquet(output_file, index=False)
        logger.info(f"✅ Parquet saved: {output_file.name}")
        
        # Validate
        if validate_parquet_file(output_file):
            logger.info(f"📊 Reddit Silver statistics:")
            logger.info(f"   - Records: {len(df)}")
            logger.info(f"   - Columns: {len(df.columns)}")
            logger.info(f"   - Size: {output_file.stat().st_size / 1024:.2f} KB")
            logger.info(f"   - Sources: {', '.join(source_files)}")
            
            return {
                "status": "success",
                "records": len(df),
                "output_file": str(output_file),
                "source_files": source_files,
                "file_size_kb": output_file.stat().st_size / 1024
            }
        else:
            raise Exception(f"Validation failed for {output_file}")
    
    # ======================== TASK 4: PROCESS LA SILLA VACÍA ========================
    @task(
        task_id="process_lasillavacia",
        doc="Preprocess La Silla Vacía data with cleaning, deduplication and validation"
    )
    def process_lasillavacia_task():
        """Get LSV files, preprocess and save as Parquet."""
        SILVER_PATH.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Search for files
        lsv_files = get_new_files("lasillavacia_*.json")
        if not lsv_files:
            logger.warning("⚠️ No La Silla Vacía files to process")
            return {"status": "skipped", "records": 0, "output_file": None}
        
        # Preprocess
        df, source_files = preprocess_lasillavacia_data(lsv_files)
        
        if df.empty:
            logger.warning("⚠️ Empty DataFrame after preprocessing")
            return {"status": "empty", "records": 0, "output_file": None}
        
        # Save as Parquet with traceability metadata
        output_file = SILVER_PATH / f"lasillavacia_{timestamp}.parquet"
        
        # Add additional metadata
        df.attrs = {
            "processed_timestamp": timestamp,
            "source_files": ",".join(source_files),
            "record_count": len(df),
            "processor": "silver_processing_dag"
        }
        
        df.to_parquet(output_file, index=False)
        logger.info(f"✅ Parquet saved: {output_file.name}")
        
        # Validate
        if validate_parquet_file(output_file):
            logger.info(f"📊 La Silla Vacía Silver statistics:")
            logger.info(f"   - Records: {len(df)}")
            logger.info(f"   - Columns: {len(df.columns)}")
            logger.info(f"   - Size: {output_file.stat().st_size / 1024:.2f} KB")
            logger.info(f"   - Sources: {', '.join(source_files)}")
            
            return {
                "status": "success",
                "records": len(df),
                "output_file": str(output_file),
                "source_files": source_files,
                "file_size_kb": output_file.stat().st_size / 1024
            }
        else:
            raise Exception(f"Validation failed for {output_file}")
    
    # ======================== TASK 5: FINAL VALIDATION ========================
    @task(
        task_id="validate_silver_layer",
        doc="Validates integrity of Parquet files in datalake_silver/"
    )
    def validate_silver_layer():
        """Verifies that generated Parquet files are valid."""
        SILVER_PATH.mkdir(parents=True, exist_ok=True)
        parquet_files = list(SILVER_PATH.glob("*.parquet"))
        
        if not parquet_files:
            logger.warning("⚠️ No Parquet files in datalake_silver/")
            return {"total_files": 0, "valid": 0, "invalid": 0}
        
        logger.info(f"🔍 Validating {len(parquet_files)} Parquet files...")
        valid_count = 0
        invalid_count = 0
        
        for pf in parquet_files:
            if validate_parquet_file(pf):
                valid_count += 1
            else:
                invalid_count += 1
        
        logger.info(f"📋 Validation completed: {valid_count} valid, {invalid_count} invalid")
        
        return {
            "total_files": len(parquet_files),
            "valid": valid_count,
            "invalid": invalid_count,
            "timestamp": datetime.now().isoformat()
        }
    
    # ======================== EXECUTION FLOW ========================
    reddit_result = process_reddit_task()
    lsv_result = process_lasillavacia_task()
    
    validation_result = validate_silver_layer()
    
    # Task dependencies
    [wait_for_reddit >> reddit_result]
    [wait_for_lsv >> lsv_result]
    [reddit_result, lsv_result] >> validation_result

# Instantiate DAG
silver_dag = silver_processing_dag()

