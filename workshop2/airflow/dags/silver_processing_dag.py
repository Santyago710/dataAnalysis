from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import json
import re
import pandas as pd
from pathlib import Path

BRONZE_PATH = "/opt/airflow/datalake_bronze"
SILVER_PATH = Path("/opt/airflow/datalake_silver")

STOPWORDS_ES = {
    "de", "la", "el", "en", "y", "a", "los", "las", "un", "una",
    "es", "se", "no", "con", "por", "que", "del", "al", "lo", "su",
    "para", "como", "más", "pero", "sus", "le", "ya", "o", "fue",
    "este", "ha", "si", "porque", "esta", "son", "entre", "cuando",
    "muy", "sin", "sobre", "también", "me", "hasta", "hay", "donde",
    "quien", "desde", "todo", "nos", "durante", "uno", "ni", "contra"
}

def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"[^\w\sáéíóúüñ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    tokens = [w for w in text.split() if w not in STOPWORDS_ES and len(w) > 2]
    return " ".join(tokens)


@dag(
    dag_id="silver_processing",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["silver", "processing"]
)
def silver_processing_dag():
    wait_for_reddit = FileSensor(
        task_id="wait_for_reddit",
        filepath=BRONZE_PATH + "/reddit_*.json",
        poke_interval=30,
        timeout=600,
        mode="poke",
        fs_conn_id="fs_default"
    )

    wait_for_lsv = FileSensor(
        task_id="wait_for_lsv",
        filepath=BRONZE_PATH + "/lasillavacia_*.json",
        poke_interval=30,
        timeout=600,
        mode="poke",
        fs_conn_id="fs_default"
    )

    @task()
    def process_reddit():
        SILVER_PATH.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Leer todos los JSON de Reddit
        bronze = Path(BRONZE_PATH)
        all_records = []
        for f in sorted(bronze.glob("reddit_*.json")):
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                for record in data:
                    record["source_file"] = f.name
                all_records.extend(data)

        if not all_records:
            print("⚠️ No hay datos de Reddit para procesar")
            return

        df = pd.DataFrame(all_records)
        print(f"📥 Reddit raw: {len(df)} registros")

        # 1. Deduplicación
        df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)

        # 2. Tipos de datos
        df["date"] = pd.to_numeric(df["date"], errors="coerce")
        df["date"] = pd.to_datetime(df["date"], unit="s", errors="coerce")
        df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype(int)

        # 3. Nulos
        df["title"] = df["title"].fillna("").str.strip()
        df["text"] = df["text"].fillna("")
        df["author"] = df["author"].fillna("unknown")
        df["source_file"] = df["source_file"].fillna("")

        # 4. Eliminar registros sin título
        df = df[df["title"].str.len() > 3].reset_index(drop=True)

        # 5. Outliers en score (IQR)
        Q1 = df["score"].quantile(0.25)
        Q3 = df["score"].quantile(0.75)
        IQR = Q3 - Q1
        df = df[df["score"] <= Q3 + 1.5 * IQR].reset_index(drop=True)

        # 6. Limpieza de texto NLP
        df["title_clean"] = df["title"].apply(clean_text)
        df["text_clean"] = df["text"].apply(clean_text)

        # 7. Schema final
        df = df[[
            "title", "title_clean", "text", "text_clean",
            "author", "date", "score", "url", "source_file"
        ]]

        output_file = SILVER_PATH / f"reddit_{timestamp}.parquet"
        df.to_parquet(output_file, index=False)
        print(f"✅ Reddit Silver: {len(df)} registros → {output_file}")
        return str(output_file)

    @task()
    def process_lasillavacia():
        SILVER_PATH.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Leer todos los JSON de La Silla Vacía
        bronze = Path(BRONZE_PATH)
        all_records = []
        for f in sorted(bronze.glob("lasillavacia_*.json")):
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                for record in data:
                    record["source_file"] = f.name
                all_records.extend(data)

        if not all_records:
            print("⚠️ No hay datos de La Silla Vacía para procesar")
            return

        df = pd.DataFrame(all_records)
        print(f"📥 La Silla Vacía raw: {len(df)} registros")

        # 1. Deduplicación
        df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)

        # 2. Tipos de datos
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

        # 3. Nulos
        df["titulo"] = df["titulo"].fillna("").str.strip()
        df["autor"] = df["autor"].fillna("Desconocido")
        df["extracto"] = df["extracto"].fillna("")
        df["contenido"] = df["contenido"].fillna("")
        df["etiquetas"] = df["etiquetas"].fillna("")
        df["source_file"] = df["source_file"].fillna("")

        # 4. Eliminar registros sin título
        df = df[df["titulo"].str.len() > 3].reset_index(drop=True)

        # 5. Limpieza de texto NLP
        df["titulo_clean"] = df["titulo"].apply(clean_text)
        df["contenido_clean"] = df["contenido"].apply(clean_text)
        df["extracto_clean"] = df["extracto"].apply(clean_text)

        # 6. Schema final
        df = df[[
            "titulo", "titulo_clean", "autor", "fecha",
            "extracto", "extracto_clean", "contenido", "contenido_clean",
            "etiquetas", "url", "fuente", "source_file"
        ]]

        output_file = SILVER_PATH / f"lasillavacia_{timestamp}.parquet"
        df.to_parquet(output_file, index=False)
        print(f"✅ La Silla Vacía Silver: {len(df)} registros → {output_file}")
        return str(output_file)

    wait_for_reddit >> process_reddit()
    wait_for_lsv >> process_lasillavacia()

silver_processing_dag()
