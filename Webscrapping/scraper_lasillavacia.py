"""La Silla Vacia Opinion scraper.

This script crawls the opinion section of La Silla Vacia and extracts article
metadata and full-text content using Playwright.

Extracted fields include:
- title
- author
- publication date
- article URL
- summary excerpt
- full article content
- tags/categories

Requirements:
    pip install playwright
    playwright install chromium
"""

import pandas as pd
import time
import logging
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ─── Configuration ─────────────────────────────────────────────────────────

BASE_URL       = "https://www.lasillavacia.com"
OPINION_URL    = f"{BASE_URL}/opinion/"
MAX_PAGINAS    = 5
DELAY          = 2          # Delay in seconds between page requests.
OUTPUT_DIR     = Path(__file__).resolve().parent / "datalake_bronze"
OUTPUT_CSV     = OUTPUT_DIR / f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.csv"
HEADLESS       = False       # False opens a visible browser window for debugging.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# ─── Listing Extraction ────────────────────────────────────────────────────

def extraer_articulos(page) -> list[dict]:
    """Extract article cards from a rendered listing page.

    The function uses resilient selector fallbacks to handle moderate HTML
    structure changes in the target site.

    Args:
        page: A Playwright page already navigated to an opinion listing.

    Returns:
        A list of dictionaries with basic article metadata.
    """
    articulos = []

    # Wait until the main content area is available.
    try:
        page.wait_for_selector("article, .card, h2 a, h3 a", timeout=10_000)
    except PlaywrightTimeout:
        log.warning("Timeout while waiting for article cards. The page may be empty.")
        return []

    # Prefer semantic <article> nodes first, then fallback selectors.
    items = page.query_selector_all("article")
    if not items:
        items = page.query_selector_all(".card, .post-card, [class*='article']")

    log.info(f"  → {len(items)} elementos encontrados en el DOM")

    for item in items:
        titulo = autor = fecha = url = extracto = ""

        # ── Title ──
        for sel in ["h1", "h2", "h3", ".title", "[class*='title']"]:
            t = item.query_selector(sel)
            if t:
                titulo = t.inner_text().strip()
                break

        # ── URL ──
        a = item.query_selector("a")
        if a:
            href = a.get_attribute("href") or ""
            url = href if href.startswith("http") else BASE_URL + href

        # ── Author ──
        for sel in [".author", ".autor", ".byline", "[class*='author']",
                    "[class*='autor']", "[rel='author']"]:
            t = item.query_selector(sel)
            if t:
                autor = t.inner_text().strip()
                break

        # ── Date ──
        for sel in ["time", "[datetime]", ".date", ".fecha", "[class*='date']"]:
            t = item.query_selector(sel)
            if t:
                fecha = t.get_attribute("datetime") or t.inner_text().strip()
                break

        # ── Excerpt ──
        for sel in ["p", ".excerpt", ".summary", "[class*='excerpt']"]:
            t = item.query_selector(sel)
            if t:
                texto = t.inner_text().strip()
                if len(texto) > 20:
                    extracto = texto[:300]
                    break

        if titulo:
            articulos.append({
                "titulo":   titulo,
                "autor":    autor,
                "fecha":    fecha,
                "url":      url,
                "extracto": extracto,
            })

    # Fallback strategy when no card containers are detected.
    if not articulos:
        log.warning("No <article> nodes detected. Trying direct h2/h3 link fallback.")
        for a in page.query_selector_all("h2 a, h3 a"):
            titulo = a.inner_text().strip()
            href   = a.get_attribute("href") or ""
            url    = href if href.startswith("http") else BASE_URL + href
            if titulo:
                articulos.append({
                    "titulo": titulo, "autor": "", "fecha": "",
                    "url": url, "extracto": ""
                })

    return articulos


def siguiente_pagina(page, pagina_actual: int) -> str | None:
    """Build or detect the URL for the next listing page.

    Args:
        page: Current Playwright page on the listing view.
        pagina_actual: Current page number.

    Returns:
        The best candidate URL for the next page, or ``None`` if no candidate
        can be inferred.
    """
    # Pattern 1: explicit rel="next" link.
    next_a = page.query_selector("a[rel='next']")
    if next_a:
        href = next_a.get_attribute("href") or ""
        return href if href.startswith("http") else BASE_URL + href

    # Pattern 2: derive URL by expected pagination format.
    siguiente = pagina_actual + 1
    for patron in [
        f"{OPINION_URL}page/{siguiente}/",
        f"{OPINION_URL}?page={siguiente}",
        f"{OPINION_URL}?paged={siguiente}",
    ]:
        enlace = page.query_selector(f"a[href*='page/{siguiente}'], a[href*='page={siguiente}']")
        if enlace:
            return patron

    return f"{OPINION_URL}page/{siguiente}/"   # Default attempt.


# ─── Article Detail Extraction ─────────────────────────────────────────────

def scrape_detalle(page, url: str) -> dict:
    """Extract full-text content and tags from an article URL.

    Args:
        page: Reusable Playwright page instance.
        url: Absolute article URL.

    Returns:
        A dictionary with:
        - ``contenido``: full article text when available.
        - ``etiquetas``: comma-separated tag list.
    """
    resultado = {"contenido": "", "etiquetas": ""}

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20_000)
        time.sleep(1)
        page.wait_for_selector("article, .content, .post-content, h1", timeout=10_000)
    except PlaywrightTimeout:
        log.warning(f"  Timeout en artículo: {url}")
        return resultado
    except Exception as e:
        log.warning(f"  Error en artículo {url}: {e}")
        return resultado

    # ── Full article content ──
    for sel in [
        ".post-content", ".entry-content", ".article-content",
        "[class*='content__body']", "[class*='article__body']",
        "[class*='story-body']", "article .body", "article"
    ]:
        el = page.query_selector(sel)
        if el:
            texto = el.inner_text().strip()
            if len(texto) > 100:
                resultado["contenido"] = texto
                break

    # ── Tags / Categories ──
    # Exact selector pattern: <span class="tags-links"> ... <a rel="tag">...
    etiquetas = []

    tags_links = page.query_selector_all(".tags-links a[rel='tag']")
    for t in tags_links:
        texto = t.inner_text().strip()
        if texto:
            etiquetas.append(texto)

    # Fallback: match any tag link present on the page.
    if not etiquetas:
        for t in page.query_selector_all("a[rel='tag']"):
            texto = t.inner_text().strip()
            if texto and texto not in etiquetas:
                etiquetas.append(texto)

    resultado["etiquetas"] = ", ".join(etiquetas)
    return resultado


# ─── Main Workflow ─────────────────────────────────────────────────────────

def main():
    """Run the end-to-end scraping workflow and export a CSV snapshot."""
    todos = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-http2",                  # Force HTTP/1.1.
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ]
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="es-CO",
            viewport={"width": 1280, "height": 800},
            extra_http_headers={
                "Accept-Language": "es-CO,es;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
            ignore_https_errors=True,
        )

        # Reduce obvious automation fingerprints.
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
        """)

        page = context.new_page()

        url_actual = OPINION_URL
        for pagina in range(1, MAX_PAGINAS + 1):
            log.info(f"Scrapeando página {pagina}: {url_actual}")
            try:
                page.goto(url_actual, wait_until="domcontentloaded", timeout=30_000)
                # Brief pause to avoid highly deterministic behavior.
                time.sleep(1.5)
            except PlaywrightTimeout:
                log.error(f"Timeout cargando página {pagina}. Deteniendo.")
                break
            except Exception as e:
                log.error(f"Error al cargar página {pagina}: {e}")
                # Persist raw HTML for troubleshooting.
                try:
                    html = page.content()
                    with open(f"debug_pagina_{pagina}.html", "w", encoding="utf-8") as f:
                        f.write(html)
                    log.info(f"HTML guardado en debug_pagina_{pagina}.html para inspección")
                except Exception:
                    pass
                break

            articulos = extraer_articulos(page)
            log.info(f"  → {len(articulos)} artículos extraídos")

            if not articulos:
                log.warning("Página vacía — deteniendo paginación.")
                break

            todos.extend(articulos)

            if pagina < MAX_PAGINAS:
                url_sig = siguiente_pagina(page, pagina)
                if not url_sig or url_sig == url_actual:
                    log.info("No hay más páginas.")
                    break
                url_actual = url_sig
                time.sleep(DELAY)

        # ── Enrich each record with full content and tags ───────────────
        total = len([a for a in todos if a.get("url")])
        log.info(f"\nDescargando contenido y etiquetas de {total} artículos...")

        for i, art in enumerate(todos, 1):
            if not art.get("url"):
                art["contenido"] = ""
                art["etiquetas"] = ""
                continue
            log.info(f"  [{i}/{total}] {art['url']}")
            detalle = scrape_detalle(page, art["url"])
            art["contenido"] = detalle["contenido"]
            art["etiquetas"] = detalle["etiquetas"]
            time.sleep(DELAY)

        browser.close()

    df = pd.DataFrame(todos)
    if not df.empty:
        df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)
        df = df[df["titulo"].str.len() > 3]

    log.info(f"\n✅ Total artículos únicos: {len(df)}")

    if df.empty:
        print("\n⚠️  No se extrajeron artículos.")
        print("   Prueba con HEADLESS = False para ver qué carga el navegador.")
    else:
        print(f"\n📄 Muestra:\n")
        print(df[["titulo", "autor", "fecha", "etiquetas"]].head(10).to_string(index=False))
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        print(f"\n💾 Guardado en: {OUTPUT_CSV}")


if __name__ == "__main__":
    print("=" * 60)
    print("  Scraper La Silla Vacía - Opinión (Playwright)")
    print(f"  Iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    main()
