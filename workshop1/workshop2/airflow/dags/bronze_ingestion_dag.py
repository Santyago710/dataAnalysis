from airflow.decorators import dag, task
from datetime import datetime
import requests
import json
import time
from pathlib import Path
from bs4 import BeautifulSoup

BRONZE_PATH = Path("/opt/airflow/datalake_bronze")
KEYWORDS = [
    "petro", "uribe", "duque", "gobierno", "presidente",
    "política", "politica", "elecciones", "votar",
    "congreso", "senado", "reforma", "corrupción",
    "corrupcion", "estado", "ley", "ministro"
]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-CO,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.google.com/",
}

def scrape_detalle(url: str) -> dict:
    """Extrae contenido completo y etiquetas de un artículo individual."""
    resultado = {"contenido": "", "etiquetas": ""}
    try:
        time.sleep(2)
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            print(f"⚠️ Detalle {url} retornó {r.status_code}")
            return resultado

        soup = BeautifulSoup(r.text, "lxml")

        # Contenido completo
        content = soup.find(class_="entry-content")
        if content:
            resultado["contenido"] = content.get_text(separator=" ", strip=True)

        # Etiquetas desde links rel=tag
        etiquetas = []
        for t in soup.find_all("a", rel="tag"):
            texto = t.get_text(strip=True)
            if texto and texto not in etiquetas:
                etiquetas.append(texto)
        resultado["etiquetas"] = ", ".join(etiquetas)

    except Exception as e:
        print(f"⚠️ Error scraping detalle {url}: {e}")

    return resultado


@dag(
    dag_id="bronze_ingestion",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion"]
)
def bronze_ingestion_dag():

    @task()
    def extract_reddit():
        BRONZE_PATH.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = BRONZE_PATH / f"reddit_{timestamp}.json"

        try:
            time.sleep(2)
            url = "https://api.reddit.com/r/Colombia/new?limit=100"
            headers = {"User-Agent": "DataAnalysisProject/1.0 (academic; udistrital)"}
            response = requests.get(url, headers=headers, timeout=20)
            print(f"Reddit status: {response.status_code}")

            if response.status_code != 200:
                print(f"⚠️ Reddit error {response.status_code}")
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump([], f)
                return str(output_file)

            posts = response.json().get("data", {}).get("children", [])
            filtered = []

            for post in posts:
                d = post.get("data", {})
                if d.get("pinned"):
                    continue
                contenido = (d.get("title", "") + " " + d.get("selftext", "")).lower()
                if len(contenido.strip()) < 20:
                    continue
                if any(k in contenido for k in KEYWORDS):
                    filtered.append({
                        "title": d.get("title", ""),
                        "text": d.get("selftext", ""),
                        "author": d.get("author", ""),
                        "date": d.get("created_utc", ""),
                        "score": d.get("score", 0),
                        "url": "https://reddit.com" + d.get("permalink", ""),
                        "source": "reddit",
                        "ingested_at": timestamp
                    })

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(filtered[:20], f, indent=4, ensure_ascii=False)

            print(f"✅ Reddit: {len(filtered)} posts guardados")

        except Exception as e:
            print(f"⚠️ Excepción Reddit: {e}")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump([], f)

        return str(output_file)

    @task()
    def extract_lasillavacia():
        BASE_URL = "https://www.lasillavacia.com"
        OPINION_URL = f"{BASE_URL}/opinion/"

        BRONZE_PATH.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = BRONZE_PATH / f"lasillavacia_{timestamp}.json"
        articulos = []

        # Paso 1: extraer listado de artículos
        for pagina in range(1, 4):
            url = OPINION_URL if pagina == 1 else f"{OPINION_URL}page/{pagina}/"
            try:
                time.sleep(3)
                response = requests.get(url, headers=HEADERS, timeout=20)
                print(f"Página {pagina} status: {response.status_code}")

                if response.status_code == 429:
                    print("⚠️ Rate limit, esperando 15s...")
                    time.sleep(15)
                    response = requests.get(url, headers=HEADERS, timeout=20)
                    if response.status_code != 200:
                        continue

                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.text, "lxml")
                items = soup.find_all("article", attrs={"data-post-id": True})
                print(f"  → {len(items)} artículos en página {pagina}")

                for item in items:
                    # Título
                    titulo = ""
                    h2 = item.find("h2", class_="entry-title")
                    if h2 and h2.find("a"):
                        titulo = h2.find("a").get_text(strip=True)

                    # URL
                    url_art = ""
                    if h2 and h2.find("a"):
                        url_art = h2.find("a").get("href", "")

                    # Autor
                    autor = ""
                    author_span = item.find("span", class_="author vcard")
                    if author_span and author_span.find("a"):
                        autor = author_span.find("a").get_text(strip=True)

                    # Fecha
                    fecha = ""
                    time_tag = item.find("time", class_="entry-date")
                    if time_tag:
                        fecha = time_tag.get("datetime", time_tag.get_text(strip=True))

                    # Extracto
                    extracto = ""
                    entry_wrapper = item.find("div", class_="entry-wrapper")
                    if entry_wrapper:
                        p = entry_wrapper.find("p")
                        if p:
                            extracto = p.get_text(strip=True)[:300]

                    # Etiquetas desde clases del article
                    clases = item.get("class", [])
                    etiquetas = [
                        c.replace("tag-", "").replace("-", " ")
                        for c in clases
                        if c.startswith("tag-")
                    ]

                    if titulo:
                        articulos.append({
                            "titulo": titulo,
                            "autor": autor,
                            "fecha": fecha,
                            "url": url_art,
                            "extracto": extracto,
                            "etiquetas": ", ".join(etiquetas),
                            "contenido": "",
                            "fuente": "lasillavacia",
                            "ingested_at": timestamp
                        })

            except Exception as e:
                print(f"⚠️ Error en página {pagina}: {e}")
                continue

        # Paso 2: enriquecer con contenido completo visitando cada artículo
        print(f"\n📥 Descargando contenido de {len(articulos)} artículos...")
        for i, art in enumerate(articulos):
            if not art.get("url"):
                continue
            print(f"  [{i+1}/{len(articulos)}] {art['url']}")
            detalle = scrape_detalle(art["url"])
            art["contenido"] = detalle["contenido"]
            # Sobreescribir etiquetas con las del artículo individual si hay
            if detalle["etiquetas"]:
                art["etiquetas"] = detalle["etiquetas"]

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(articulos, f, indent=4, ensure_ascii=False)

        print(f"✅ La Silla Vacía: {len(articulos)} artículos guardados")
        return str(output_file)

    reddit_file = extract_reddit()
    lsv_file = extract_lasillavacia()

bronze_ingestion_dag()