# Airflow DAG for ingesting data into the Bronze layer.
# It extracts information from Reddit and La Silla Vacia, applying basic filters
# and validations. It stores raw data in JSON format for later processing.

from airflow.decorators import dag, task
from datetime import datetime
import requests
import json
import time
from pathlib import Path
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Global configuration
# ---------------------------------------------------------------------------

BRONZE_PATH = Path("/opt/airflow/datalake_bronze")

KEYWORDS = [
    "petro", "uribe", "duque", "gobierno", "presidente",
    "politica", "politica", "elecciones", "votar",
    "congreso", "senado", "reforma", "corrupcion",
    "corrupcion", "estado", "ley", "ministro",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-CO,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.google.com/",
}


# ---------------------------------------------------------------------------
# Shared helper function
# ---------------------------------------------------------------------------

def scrape_detalle(url):
    """Extract full content and tags from a single article."""
    resultado = {"contenido": "", "etiquetas": ""}
    try:
        time.sleep(2)
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            print("Detalle " + url + " retorno " + str(r.status_code))
            return resultado

        soup = BeautifulSoup(r.text, "lxml")

        content = soup.find(class_="entry-content")
        if content:
            resultado["contenido"] = content.get_text(separator=" ", strip=True)

        etiquetas = []
        for t in soup.find_all("a", rel="tag"):
            texto = t.get_text(strip=True)
            if texto and texto not in etiquetas:
                etiquetas.append(texto)
        resultado["etiquetas"] = ", ".join(etiquetas)

    except Exception as e:
        print("Error scraping detalle " + url + ": " + str(e))

    return resultado


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="bronze_ingestion",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion"],
)
def bronze_ingestion_dag():

    # -----------------------------------------------------------------------
    # Task 1: Reddit
    # -----------------------------------------------------------------------

    @task()
    def extract_reddit():
        BRONZE_PATH.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = BRONZE_PATH / ("reddit_" + timestamp + ".json")

        try:
            time.sleep(2)
            url = "https://api.reddit.com/r/Colombia/new?limit=100"
            headers = {"User-Agent": "DataAnalysisProject/1.0 (academic; udistrital)"}
            response = requests.get(url, headers=headers, timeout=20)
            print("Reddit status: " + str(response.status_code))

            if response.status_code != 200:
                print("Reddit error " + str(response.status_code))
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
                        "ingested_at": timestamp,
                    })

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(filtered[:20], f, indent=4, ensure_ascii=False)

            print("Reddit: " + str(len(filtered)) + " posts guardados")

        except Exception as e:
            print("Excepcion Reddit: " + str(e))
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump([], f)

        return str(output_file)

    # -----------------------------------------------------------------------
    # Task 2: La Silla Vacia
    # -----------------------------------------------------------------------

    @task()
    def extract_lasillavacia():

        BASE_URL = "https://www.lasillavacia.com"
        SECTIONS = ["/opinion/", "/silla-llena/", "/analisis/"]
        MAX_PAGES = 10
        MAX_ARTICULOS = 150
        DELAY_PAGE = 3
        MAX_RETRIES = 3

        BRONZE_PATH.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = BRONZE_PATH / ("lasillavacia_" + timestamp + ".json")
        articulos = []
        urls_vistas = set()

        # GET with retries and exponential backoff
        def fetch_with_retry(url, delay=DELAY_PAGE):
            for intento in range(1, MAX_RETRIES + 1):
                try:
                    time.sleep(delay)
                    resp = requests.get(url, headers=HEADERS, timeout=20)
                    if resp.status_code == 200:
                        return resp
                    if resp.status_code == 429:
                        espera = 15 * intento
                        print("Rate limit en " + url + ", esperando " + str(espera) + "s (intento " + str(intento) + ")")
                        time.sleep(espera)
                        continue
                    print("HTTP " + str(resp.status_code) + " en " + url + " (intento " + str(intento) + ")")
                except Exception as e:
                    print("Error en " + url + " intento " + str(intento) + ": " + str(e))
                    time.sleep(5 * intento)
            return None

        # Parse articles from a listing page
        def parsear_pagina(soup):
            items = soup.find_all("article", attrs={"data-post-id": True})
            resultado = []

            for item in items:
                url_art = ""
                h2 = item.find("h2", class_="entry-title")
                if h2 and h2.find("a"):
                    url_art = h2.find("a").get("href", "").strip()
                if not url_art or url_art in urls_vistas:
                    continue
                urls_vistas.add(url_art)

                titulo = h2.find("a").get_text(strip=True) if h2 and h2.find("a") else ""

                autor = ""
                author_span = item.find("span", class_="author vcard")
                if author_span and author_span.find("a"):
                    autor = author_span.find("a").get_text(strip=True)

                fecha = ""
                time_tag = item.find("time", class_="entry-date")
                if time_tag:
                    fecha = time_tag.get("datetime", time_tag.get_text(strip=True))

                extracto = ""
                entry_wrapper = item.find("div", class_="entry-wrapper")
                if entry_wrapper:
                    p = entry_wrapper.find("p")
                    if p:
                        extracto = p.get_text(strip=True)[:300]

                clases = item.get("class", [])
                etiquetas = [
                    c.replace("tag-", "").replace("-", " ")
                    for c in clases
                    if c.startswith("tag-")
                ]

                if titulo:
                    resultado.append({
                        "titulo": titulo,
                        "autor": autor,
                        "fecha": fecha,
                        "url": url_art,
                        "extracto": extracto,
                        "etiquetas": ", ".join(etiquetas),
                        "contenido": "",
                        "fuente": "lasillavacia",
                        "ingested_at": timestamp,
                    })

            return resultado

        # Detect if a next page exists
        def hay_siguiente(soup):
            nav = (
                soup.find("div", class_="nav-links")
                or soup.find("nav", class_="pagination")
            )
            if nav and nav.find("a", class_="next"):
                return True
            for a in soup.find_all("a", class_="page-numbers"):
                if "next" in a.get("class", []):
                    return True
            return False

        # Scrape listings by section and page
        for seccion in SECTIONS:
            print("\nSeccion: " + seccion)

            for pagina in range(1, MAX_PAGES + 1):
                if len(articulos) >= MAX_ARTICULOS:
                    print("Limite de " + str(MAX_ARTICULOS) + " articulos alcanzado.")
                    break

                if pagina == 1:
                    url_pag = BASE_URL + seccion
                else:
                    url_pag = BASE_URL + seccion + "page/" + str(pagina) + "/"

                print("Pagina " + str(pagina) + ": " + url_pag)

                resp = fetch_with_retry(url_pag, delay=DELAY_PAGE)
                if resp is None:
                    print("No se pudo obtener pagina " + str(pagina) + ", saltando seccion.")
                    break

                soup = BeautifulSoup(resp.text, "lxml")
                nuevos = parsear_pagina(soup)
                articulos.extend(nuevos)
                print("+" + str(len(nuevos)) + " articulos | total=" + str(len(articulos)))

                if len(nuevos) == 0:
                    print("Sin articulos nuevos en pagina " + str(pagina) + ", fin de seccion.")
                    break

                if not hay_siguiente(soup):
                    print("No hay pagina siguiente, fin de seccion.")
                    break

        # Download full content for each article
        print("\nDescargando contenido de " + str(len(articulos)) + " articulos...")
        for i, art in enumerate(articulos):
            if not art.get("url"):
                continue
            print("[" + str(i + 1) + "/" + str(len(articulos)) + "] " + art["url"])
            detalle = scrape_detalle(art["url"])
            art["contenido"] = detalle["contenido"]
            if detalle["etiquetas"]:
                art["etiquetas"] = detalle["etiquetas"]
            if not art["extracto"] and art["contenido"]:
                art["extracto"] = art["contenido"][:300]

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(articulos, f, indent=4, ensure_ascii=False)

        print("La Silla Vacia: " + str(len(articulos)) + " articulos guardados en " + str(output_file))
        return str(output_file)

    # -----------------------------------------------------------------------
    # Task 3: Validation
    # -----------------------------------------------------------------------

    @task()
    def validate_data(reddit_file, lsv_file):
        REQUIRED_REDDIT = {"title", "author", "date", "url", "source"}
        REQUIRED_LSV = {"titulo", "autor", "fecha", "url", "contenido"}
        errors = []

        with open(reddit_file, "r", encoding="utf-8") as f:
            reddit_data = json.load(f)
        print("Reddit: " + str(len(reddit_data)) + " registros")
        for i, record in enumerate(reddit_data):
            missing = REQUIRED_REDDIT - set(record.keys())
            if missing:
                errors.append("Reddit record " + str(i) + " missing fields: " + str(missing))
            if not record.get("title", "").strip():
                errors.append("Reddit record " + str(i) + " has empty title")

        with open(lsv_file, "r", encoding="utf-8") as f:
            lsv_data = json.load(f)
        print("La Silla Vacia: " + str(len(lsv_data)) + " registros")
        for i, record in enumerate(lsv_data):
            missing = REQUIRED_LSV - set(record.keys())
            if missing:
                errors.append("LSV record " + str(i) + " missing fields: " + str(missing))
            if not record.get("titulo", "").strip():
                errors.append("LSV record " + str(i) + " has empty titulo")

        if errors:
            print("Validation warnings (" + str(len(errors)) + "):")
            for e in errors:
                print("  - " + e)
        else:
            print("Validation passed - all records have required fields")

        return {
            "reddit_count": len(reddit_data),
            "lsv_count": len(lsv_data),
            "warnings": len(errors),
        }

    # -----------------------------------------------------------------------
    # Orchestration
    # -----------------------------------------------------------------------
    reddit_file = extract_reddit()
    lsv_file = extract_lasillavacia()
    validate_data(reddit_file, lsv_file)


bronze_ingestion_dag()