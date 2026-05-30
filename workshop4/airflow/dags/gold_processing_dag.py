from airflow.decorators import dag, task
from datetime import datetime
from pathlib import Path

# Base storage paths for curated (Gold) and cleaned (Silver) layers.
SILVER_PATH = "/opt/airflow/datalake_silver"
GOLD_PATH = "/opt/airflow/datalake_gold"

# Weekly Gold pipeline that creates governance and storytelling outputs.
@dag(
    dag_id="gold_processing",
    schedule_interval="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "pyspark"]
)
def gold_processing_dag():

    @task()
    def governance_summary():
        # Local imports keep task dependencies explicit and isolated.
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pathlib import Path
        import pandas as pd

        # Initialize a local Spark session optimized for small batch workloads.
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("GoldGovernance") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .getOrCreate()

        # Reduce verbosity and prepare output paths.
        spark.sparkContext.setLogLevel("ERROR")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        Path(GOLD_PATH).mkdir(parents=True, exist_ok=True)
        results = []

        # Compute governance metrics for Reddit data if available.
        reddit_files = list(Path(SILVER_PATH).glob("reddit_*.parquet"))
        if reddit_files:
            df_reddit = spark.read.parquet(*[str(f) for f in reddit_files])
            total = df_reddit.count()
            null_title = df_reddit.filter(F.col("title").isNull() | (F.col("title") == "")).count()
            null_author = df_reddit.filter(F.col("author").isNull() | (F.col("author") == "")).count()
            null_text = df_reddit.filter(F.col("text").isNull() | (F.col("text") == "")).count()
            null_url = df_reddit.filter(F.col("url").isNull() | (F.col("url") == "")).count()
            dup_count = total - df_reddit.dropDuplicates(["url"]).count()
            # Basic score distribution and title-length stats.
            score_stats = df_reddit.select(
                F.mean("score").alias("score_mean"),
                F.expr("percentile(score, 0.5)").alias("score_median"),
                F.min("score").alias("score_min"),
                F.max("score").alias("score_max")
            ).collect()[0]
            title_len = df_reddit.select(
                F.mean(F.length("title")).alias("title_len_mean"),
                F.min(F.length("title")).alias("title_len_min"),
                F.max(F.length("title")).alias("title_len_max")
            ).collect()[0]
            # Consolidate governance metrics into a single record.
            results.append({
                "source": "reddit",
                "total_records": total,
                "null_rate_title": round(null_title / total * 100, 2) if total > 0 else 0,
                "null_rate_author": round(null_author / total * 100, 2) if total > 0 else 0,
                "null_rate_text": round(null_text / total * 100, 2) if total > 0 else 0,
                "null_rate_url": round(null_url / total * 100, 2) if total > 0 else 0,
                "duplicate_rate": round(dup_count / total * 100, 2) if total > 0 else 0,
                "score_mean": float(score_stats["score_mean"] or 0),
                "score_median": float(score_stats["score_median"] or 0),
                "score_min": int(score_stats["score_min"] or 0),
                "score_max": int(score_stats["score_max"] or 0),
                "title_len_mean": float(title_len["title_len_mean"] or 0),
                "title_len_min": int(title_len["title_len_min"] or 0),
                "title_len_max": int(title_len["title_len_max"] or 0),
                "computed_at": timestamp
            })
            print(f"✅ Reddit governance: {total} registros procesados")

        # Compute governance metrics for La Silla Vacia data if available.
        lsv_files = list(Path(SILVER_PATH).glob("lasillavacia_*.parquet"))
        if lsv_files:
            df_lsv = spark.read.parquet(*[str(f) for f in lsv_files])
            total = df_lsv.count()
            null_titulo = df_lsv.filter(F.col("titulo").isNull() | (F.col("titulo") == "")).count()
            null_autor = df_lsv.filter(F.col("autor").isNull() | (F.col("autor") == "")).count()
            null_contenido = df_lsv.filter(F.col("contenido").isNull() | (F.col("contenido") == "")).count()
            null_url = df_lsv.filter(F.col("url").isNull() | (F.col("url") == "")).count()
            dup_count = total - df_lsv.dropDuplicates(["url"]).count()
            contenido_len = df_lsv.select(
                F.mean(F.length("contenido")).alias("contenido_len_mean"),
                F.min(F.length("contenido")).alias("contenido_len_min"),
                F.max(F.length("contenido")).alias("contenido_len_max")
            ).collect()[0]
            # Align schema with Reddit governance output for reporting consistency.
            results.append({
                "source": "lasillavacia",
                "total_records": total,
                "null_rate_title": round(null_titulo / total * 100, 2) if total > 0 else 0,
                "null_rate_author": round(null_autor / total * 100, 2) if total > 0 else 0,
                "null_rate_text": round(null_contenido / total * 100, 2) if total > 0 else 0,
                "null_rate_url": round(null_url / total * 100, 2) if total > 0 else 0,
                "duplicate_rate": round(dup_count / total * 100, 2) if total > 0 else 0,
                "score_mean": 0.0,
                "score_median": 0.0,
                "score_min": 0,
                "score_max": 0,
                "title_len_mean": float(contenido_len["contenido_len_mean"] or 0),
                "title_len_min": int(contenido_len["contenido_len_min"] or 0),
                "title_len_max": int(contenido_len["contenido_len_max"] or 0),
                "computed_at": timestamp
            })
            print(f"✅ La Silla Vacia governance: {total} registros procesados")

        # Persist governance metrics as a single Gold parquet file.
        df_gov = pd.DataFrame(results)
        output = f"{GOLD_PATH}/governance_{timestamp}.parquet"
        df_gov.to_parquet(output, index=False)
        print(f"✅ Governance saved: {output}")
        spark.stop()
        return output

    @task()
    def storytelling_summary():
        # Local imports keep task dependencies explicit and isolated.
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, FloatType
        from pathlib import Path
        from functools import reduce
        from pyspark.sql import DataFrame
        import pandas as pd

        # Initialize a local Spark session optimized for small batch workloads.
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("GoldStorytelling") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .getOrCreate()

        # Reduce verbosity and prepare output paths.
        spark.sparkContext.setLogLevel("ERROR")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        Path(GOLD_PATH).mkdir(parents=True, exist_ok=True)
        all_dfs = []

        # Normalize Reddit schema to the storytelling model.
        reddit_files = list(Path(SILVER_PATH).glob("reddit_*.parquet"))
        if reddit_files:
            df = spark.read.parquet(*[str(f) for f in reddit_files])
            df = df.withColumn("source", F.lit("reddit"))
            df = df.withColumn("text_combined", F.concat_ws(" ", F.col("title_clean"), F.col("text_clean")))
            df = df.withColumn("fecha", F.to_date(F.col("date").cast("timestamp")))
            df_sel = df.select(
                F.col("title").alias("titulo"),
                F.col("text_combined").alias("contenido_clean"),
                F.col("author").alias("autor"),
                F.col("fecha"),
                F.col("score"),
                F.col("url"),
                F.col("source")
            )
            all_dfs.append(df_sel)

        # Normalize La Silla Vacia schema to the storytelling model.
        lsv_files = list(Path(SILVER_PATH).glob("lasillavacia_*.parquet"))
        if lsv_files:
            df = spark.read.parquet(*[str(f) for f in lsv_files])
            df = df.withColumn("source", F.lit("lasillavacia"))
            df = df.withColumn("score", F.lit(0))
            df = df.withColumn("fecha", F.to_date(F.col("fecha").cast("timestamp")))
            df_sel = df.select(
                F.col("titulo"),
                F.col("contenido_clean"),
                F.col("autor"),
                F.col("fecha"),
                F.col("score"),
                F.col("url"),
                F.col("source")
            )
            all_dfs.append(df_sel)

        # Exit early when there is no Silver data to process.
        if not all_dfs:
            print("⚠️ No hay datos Silver para procesar")
            spark.stop()
            return

        # Union all sources and remove duplicate URLs.
        df_all = reduce(DataFrame.union, all_dfs)
        df_all = df_all.dropDuplicates(["url"])

        # Local sentiment functions based on simple word dictionaries.
        def analyze_sentiment_local(text):
            if not isinstance(text, str) or len(text.strip()) == 0:
                return "neutral"
            positive_words = {
                "bien", "bueno", "buena", "excelente", "positivo", "positiva",
                "éxito", "logro", "avance", "progreso", "esperanza", "mejor",
                "desarrollo", "beneficio", "apoyo", "favor", "correcto", "justo",
                "paz", "acuerdo", "solución", "oportunidad", "crecimiento",
                "inversión", "empleo", "salud", "educación", "seguridad",
                "victoria", "triunfo", "alegría", "libre", "libertad",
                "democracia", "transparencia", "honesto"
            }
            negative_words = {
                "malo", "mala", "mal", "terrible", "pésimo", "negativo",
                "fracaso", "problema", "crisis", "corrupción", "corrupto",
                "robo", "mentira", "fraude", "violencia", "guerra", "conflicto",
                "pobreza", "desempleo", "inflación", "escándalo", "injusto",
                "ilegal", "criminal", "crimen", "impunidad", "dictadura",
                "represión", "desastre", "caos", "peor", "desigualdad",
                "hambre", "miseria", "muerte", "asesinato", "secuestro"
            }
            words = text.lower().split()
            pos = sum(1 for w in words if w in positive_words)
            neg = sum(1 for w in words if w in negative_words)
            total = pos + neg
            if total == 0:
                return "neutral"
            polarity = (pos - neg) / total
            if polarity > 0.1:
                return "positive"
            elif polarity < -0.1:
                return "negative"
            return "neutral"

        # Polarity score for trend analysis (-1 to 1).
        def get_polarity_local(text):
            if not isinstance(text, str) or len(text.strip()) == 0:
                return 0.0
            positive_words = {
                "bien", "bueno", "buena", "excelente", "positivo", "positiva",
                "éxito", "logro", "avance", "progreso", "esperanza", "mejor",
                "desarrollo", "beneficio", "apoyo", "favor", "correcto", "justo",
                "paz", "acuerdo", "solución", "oportunidad", "crecimiento",
                "inversión", "empleo", "salud", "educación", "seguridad",
                "victoria", "triunfo", "alegría", "libre", "libertad",
                "democracia", "transparencia", "honesto"
            }
            negative_words = {
                "malo", "mala", "mal", "terrible", "pésimo", "negativo",
                "fracaso", "problema", "crisis", "corrupción", "corrupto",
                "robo", "mentira", "fraude", "violencia", "guerra", "conflicto",
                "pobreza", "desempleo", "inflación", "escándalo", "injusto",
                "ilegal", "criminal", "crimen", "impunidad", "dictadura",
                "represión", "desastre", "caos", "peor", "desigualdad",
                "hambre", "miseria", "muerte", "asesinato", "secuestro"
            }
            words = text.lower().split()
            pos = sum(1 for w in words if w in positive_words)
            neg = sum(1 for w in words if w in negative_words)
            total = pos + neg
            if total == 0:
                return 0.0
            return round((pos - neg) / total, 4)

        # Register UDFs for sentiment labeling and polarity scoring.
        sentiment_udf = F.udf(analyze_sentiment_local, StringType())
        polarity_udf = F.udf(get_polarity_local, FloatType())

        # Enrich with sentiment features.
        df_all = df_all.withColumn("sentiment", sentiment_udf(F.col("contenido_clean")))
        df_all = df_all.withColumn("polarity", polarity_udf(F.col("contenido_clean")))

        # 1. Volume by source and date.
        volume_trend = df_all.groupBy("source", "fecha") \
            .agg(F.count("*").alias("record_count")) \
            .orderBy("fecha") \
            .toPandas()

        # 2. Top keywords by frequency.
        words_df = df_all.select(
            F.explode(F.split(F.col("contenido_clean"), " ")).alias("word"),
            F.col("source")
        ).filter(F.length("word") > 3)
        top_keywords = words_df.groupBy("word", "source") \
            .agg(F.count("*").alias("frequency")) \
            .orderBy(F.desc("frequency")) \
            .limit(50) \
            .toPandas()

        # 3. Average Reddit score by date.
        score_trend = df_all.filter(F.col("source") == "reddit") \
            .groupBy("fecha") \
            .agg(
                F.mean("score").alias("avg_score"),
                F.count("*").alias("post_count")
            ) \
            .orderBy("fecha") \
            .toPandas()

        # 4. Top authors by source.
        top_authors = df_all.groupBy("autor", "source") \
            .agg(F.count("*").alias("article_count")) \
            .orderBy(F.desc("article_count")) \
            .limit(20) \
            .toPandas()

        # 5. Sentiment distribution by source.
        sentiment_dist = df_all.groupBy("source", "sentiment") \
            .agg(F.count("*").alias("count")) \
            .toPandas()

        # 6. Sentiment trend by date and source.
        sentiment_trend = df_all.groupBy("fecha", "source") \
            .agg(
                F.mean("polarity").alias("avg_polarity"),
                F.count("*").alias("record_count")
            ) \
            .orderBy("fecha") \
            .toPandas()

        # Persist all storytelling datasets to Gold.
        volume_trend.to_parquet(f"{GOLD_PATH}/storytelling_volume_{timestamp}.parquet", index=False)
        top_keywords.to_parquet(f"{GOLD_PATH}/storytelling_keywords_{timestamp}.parquet", index=False)
        score_trend.to_parquet(f"{GOLD_PATH}/storytelling_score_trend_{timestamp}.parquet", index=False)
        top_authors.to_parquet(f"{GOLD_PATH}/storytelling_authors_{timestamp}.parquet", index=False)
        sentiment_dist.to_parquet(f"{GOLD_PATH}/storytelling_sentiment_dist_{timestamp}.parquet", index=False)
        sentiment_trend.to_parquet(f"{GOLD_PATH}/storytelling_sentiment_trend_{timestamp}.parquet", index=False)

        print(f"✅ Storytelling saved: 6 archivos en {GOLD_PATH}")
        spark.stop()
        return f"{GOLD_PATH}/storytelling_volume_{timestamp}.parquet"

    # Run both Gold tasks (independent execution paths).
    governance_summary()
    storytelling_summary()

# Register the DAG with Airflow.
gold_processing_dag()