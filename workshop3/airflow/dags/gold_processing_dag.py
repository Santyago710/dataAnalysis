from airflow.decorators import dag, task
from datetime import datetime
from pathlib import Path

SILVER_PATH = "/opt/airflow/datalake_silver"
GOLD_PATH = "/opt/airflow/datalake_gold"

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
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pathlib import Path

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

        spark.sparkContext.setLogLevel("ERROR")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        Path(GOLD_PATH).mkdir(parents=True, exist_ok=True)

        results = []

        # Reddit
        reddit_files = list(Path(SILVER_PATH).glob("reddit_*.parquet"))
        if reddit_files:
            df_reddit = spark.read.parquet(*[str(f) for f in reddit_files])
            total = df_reddit.count()
            null_title = df_reddit.filter(F.col("title").isNull() | (F.col("title") == "")).count()
            null_author = df_reddit.filter(F.col("author").isNull() | (F.col("author") == "")).count()
            null_text = df_reddit.filter(F.col("text").isNull() | (F.col("text") == "")).count()
            null_url = df_reddit.filter(F.col("url").isNull() | (F.col("url") == "")).count()
            dup_count = total - df_reddit.dropDuplicates(["url"]).count()

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

        # La Silla Vacia
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

        import pandas as pd
        df_gov = pd.DataFrame(results)
        output = f"{GOLD_PATH}/governance_{timestamp}.parquet"
        df_gov.to_parquet(output, index=False)
        print(f"✅ Governance saved: {output}")
        spark.stop()
        return output

    @task()
    def storytelling_summary():
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pathlib import Path
        from functools import reduce
        from pyspark.sql import DataFrame
        import pandas as pd

        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("GoldStorytelling") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        Path(GOLD_PATH).mkdir(parents=True, exist_ok=True)

        all_dfs = []

        # Reddit
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

        # La Silla Vacia
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

        if not all_dfs:
            print("⚠️ No hay datos Silver para procesar")
            spark.stop()
            return

        df_all = reduce(DataFrame.union, all_dfs)
        df_all = df_all.dropDuplicates(["url"])

        # 1. Volumen por fuente y fecha
        volume_trend = df_all.groupBy("source", "fecha") \
            .agg(F.count("*").alias("record_count")) \
            .orderBy("fecha") \
            .toPandas()

        # 2. Top keywords
        words_df = df_all.select(
            F.explode(F.split(F.col("contenido_clean"), " ")).alias("word"),
            F.col("source")
        ).filter(F.length("word") > 3)

        top_keywords = words_df.groupBy("word", "source") \
            .agg(F.count("*").alias("frequency")) \
            .orderBy(F.desc("frequency")) \
            .limit(50) \
            .toPandas()

        # 3. Score promedio por fecha (Reddit)
        score_trend = df_all.filter(F.col("source") == "reddit") \
            .groupBy("fecha") \
            .agg(
                F.mean("score").alias("avg_score"),
                F.count("*").alias("post_count")
            ) \
            .orderBy("fecha") \
            .toPandas()

        # 4. Top autores por fuente
        top_authors = df_all.groupBy("autor", "source") \
            .agg(F.count("*").alias("article_count")) \
            .orderBy(F.desc("article_count")) \
            .limit(20) \
            .toPandas()

        volume_trend.to_parquet(f"{GOLD_PATH}/storytelling_volume_{timestamp}.parquet", index=False)
        top_keywords.to_parquet(f"{GOLD_PATH}/storytelling_keywords_{timestamp}.parquet", index=False)
        score_trend.to_parquet(f"{GOLD_PATH}/storytelling_score_trend_{timestamp}.parquet", index=False)
        top_authors.to_parquet(f"{GOLD_PATH}/storytelling_authors_{timestamp}.parquet", index=False)

        print(f"✅ Storytelling saved: 4 archivos en {GOLD_PATH}")
        spark.stop()
        return f"{GOLD_PATH}/storytelling_volume_{timestamp}.parquet"

    governance_summary()
    storytelling_summary()

gold_processing_dag()