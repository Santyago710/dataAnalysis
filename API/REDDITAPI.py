# ╔═══════════════════════════════════════════════════════════════╗
# ║           IMPORTACIONES - Librerías necesarias                 ║
# ╚═══════════════════════════════════════════════════════════════╝

import requests  # Librería para realizar peticiones HTTP (GET, POST, etc.)
import json  # Librería para trabajar y convertir datos en formato JSON
from datetime import datetime  # Para obtener fecha y hora actual del sistema
from pathlib import Path  # Para trabajar con rutas de archivos de forma multiplataforma

# ─── Configuración ─────────────────────────────

# URL DE LA API DE REDDIT
# r/Colombia: accede al subreddit específico de Colombia
# new: obtiene los posts más recientes
# limit=100: limita la respuesta a máximo 100 posts
url = "https://api.reddit.com/r/Colombia/new?limit=100" 

# HEADERS HTTP - Información que enviamos al servidor
# User-Agent: simula que la petición viene desde un navegador real (Mozilla Firefox)
# Sin esto, Reddit podría rechazar nuestra petición por ser un bot/script
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# PALABRAS CLAVE - Términos políticos para filtrar posts relevantes
# Lista de palabras que buscamos en los títulos y contenido de los posts
keywords = [
    "petro", "uribe", "duque",              # Nombres de políticos colombianos
    "gobierno", "presidente",                # Términos sobre autoridades
    "política", "politica",                  # Tema principal (con y sin acento)
    "elecciones", "votar",                   # Procesos electorales
    "congreso", "senado",                    # Instituciones políticas
    "reforma", "corrupción",                 # Cambios y problemas políticos
    "corrupcion", "estado",                  # Variantes sin acento y poder estatal
    "ley", "ministro"                        # Legislación y cargos públicos
]

# ─── Petición HTTP a Reddit ─────────────────────────────

# Realizamos una petición GET a la URL de Reddit
# response: objeto que contiene la respuesta del servidor (código, headers, contenido)
response = requests.get(url, headers=headers)

# VALIDACIÓN 1: Verifica que la petición fue exitosa
# status_code 200 = OK (petición correcta)
# status_code 404 = No encontrado
# status_code 403 = Acceso denegado
# status_code 429 = Demasiadas peticiones (bloqueado temporalmente)
if response.status_code != 200:
    # Si la petición falla, imprime el código de error
    print("❌ Error en la petición:", response.status_code)
    # Muestra los primeros 300 caracteres de la respuesta para debug
    print(response.text[:300])
    # exit() termina completamente el programa
    exit()

# VALIDACIÓN 2: Intenta convertir la respuesta a formato JSON
# Si la respuesta no es JSON válido, esto genera una excepción
try:
    # response.json() convierte el texto JSON a diccionarios y listas de Python
    data = response.json()
except Exception as e:
    # Si hay error en la conversión, captura la excepción
    print("❌ Error al convertir a JSON:", e)
    # Muestra parte de la respuesta problematica
    print(response.text[:300])
    # Termina el programa
    exit()

# EXTRACCIÓN de posts de la estructura JSON
# Reddit devuelve: {"data": {"children": [{"data": {...}}, {"data": {...}}]}}
# .get("data", {}) obtiene el campo "data", si no existe devuelve {} (diccionario vacío)
# .get("children", []) obtiene el campo "children" de data, si no existe devuelve [] (lista vacía)
# Resultado: una lista con todos los posts obtenidos
posts = data.get("data", {}).get("children", [])

# INICIALIZACIÓN: Lista vacía donde guardaremos los posts filtrados (políticos)
filtered_posts = []

# ─── Filtrado de Posts ─────────────────────────────

# BUCLE: Itera sobre cada post obtenido de Reddit
for post in posts:
    # Extrae el diccionario "data" que contiene toda la información del post
    # Si no existe "data", utiliza un diccionario vacío {}
    data_post = post.get("data", {})

    # FILTRO 1: Ignora posts "fijados" (stickied/pinned)
    # Los posts fijados son anuncios o avisos importantes del moderador
    # data_post.get("pinned"): retorna True/False, None si no existe
    if data_post.get("pinned"):
        # continue: salta este post y va al siguiente en el bucle
        continue

    # EXTRACCIÓN DE TEXTO: Obtiene título y contenido para buscar palabras clave
    # .lower(): convierte a minúsculas para hacer comparación sin importar mayúsculas
    # Ejemplo: "POLÍTICA" y "política" se tratarán como iguales
    title = data_post.get("title", "").lower()
    text = data_post.get("selftext", "").lower()

    # COMBINACIÓN: Une título y contenido para hacer una búsqueda más completa
    # Separamos con espacio para evitar que palabras se junten
    contenido = title + " " + text

    # FILTRO 2: Evita posts vacíos o muy cortos (menos de 20 caracteres)
    # .strip(): elimina espacios en blanco al inicio y final
    # len(): cuenta el número de caracteres
    if len(contenido.strip()) < 20:
        # Salta posts muy cortos que probablemente no son relevantes
        continue

    # FILTRO 3: Busca si ALGUNA palabra clave está en el contenido
    # any(): retorna True si AL MENOS UNA condición es verdadera
    # (k in contenido for k in keywords): verifica cada palabra clave
    # Ejemplo: if "petro" in contenido or "uribe" in contenido or ...
    if any(k in contenido for k in keywords):
        # Si encuentra al menos una palabra clave, agrega el post a la lista filtrada
        # Creamos un diccionario con los datos principales del post
        filtered_posts.append({
            "title": data_post.get("title", ""),              # Título original (sin minúsculas)
            "text": data_post.get("selftext", ""),            # Cuerpo del post (si es texto)
            "author": data_post.get("author", ""),            # Usuario que lo publicó
            "date": data_post.get("created_utc", ""),         # Fecha de creación (timestamp Unix)
            "score": data_post.get("score", 0),               # Puntos (upvotes - downvotes)
            # Construye la URL completa al post de Reddit
            "url": "https://reddit.com" + data_post.get("permalink", "")
        })

# ─── Estadísticas ─────────────────────────────

# Imprime el número total de posts obtenidos de Reddit (máximo 100)
print(" Posts totales:", len(posts))
# Imprime cuántos posts contienen palabras políticas (después del filtrado)
print(" Posts políticos:", len(filtered_posts))

# ─── Ruta de salida ─────────────────────────────

# DEFINICIÓN DE RUTA: Define dónde guardar los archivos JSON
# Path(): crea un objeto de ruta que funciona en Windows, Linux y Mac
# r"...": raw string (cadena cruda) - interpreta las barras \ como literales, no como escapes
# Esta es la carpeta donde se guardarán todos los archivos descargados
output_path = Path(r"C:\Users\santy\OneDrive - Universidad Distrital Francisco José de Caldas\Documentos\Universidad\2026-1\dataAnalysis\dataAnalysis\datalake_bronze\dataAPI")

# CREACIÓN DE CARPETA: Crea la carpeta si no existe
# parents=True: crea también las carpetas padres (más de un nivel)
# exist_ok=True: no da error si la carpeta ya existe (evita que falle if ya existe)
output_path.mkdir(parents=True, exist_ok=True)

# GENERACIÓN DE TIMESTAMP: Crea un nombre único con fecha y hora actual
# datetime.now(): obtiene la fecha y hora actual del sistema
# strftime(): formatea la fecha según el patrón dado
# "%Y%m%d_%H%M%S" = AÑO MES DÍA_HORA MINUTO SEGUNDO
# Ejemplo: "20260321_143045" = 21 de marzo de 2026 a las 14:30:45
fecha = datetime.now().strftime("%Y%m%d_%H%M%S")

# CONSTRUCCIÓN DEL NOMBRE DE ARCHIVO
# output_path / f"..." : operador / de Path para concatenar rutas
# f"...": f-string que interpola variables dentro de llaves {}
# Resultado: "...DataAPI\reddit_colombia_20260321_143045.json"
filename = output_path / f"reddit_colombia_{fecha}.json"

# ─── Guardar JSON ─────────────────────────────

# CONDICIONAL: Verifica si se encontraron posts políticos
if len(filtered_posts) == 0:
    # Si no hay posts filtrados, muestra advertencia y no crea archivo
    print("⚠️ No se encontraron posts políticos")
else:
    # Si hay posts, procede a guardar en archivo
    
    # APERTURA DE ARCHIVO: Abre el archivo en modo escritura
    # with open(): gestor de contexto que cierra automáticamente el archivo al terminar
    # "w": modo escritura (sobrescribe si existe)
    # encoding="utf-8": permite caracteres especiales (ñ, acentos, emojis)
    with open(filename, "w", encoding="utf-8") as f:
        # SERIALIZACIÓN JSON: Convierte la lista de posts a JSON y escribe en archivo
        json.dump(
            filtered_posts[:20],        # Toma solo los primeros 20 posts (limita tamaño)
            f,                          # f: el archivo donde escribir
            indent=4,                   # Formatea con 4 espacios (legible, no comprimido)
            ensure_ascii=False          # Mantiene caracteres latinos sin escapar (\u00f1 vs ñ)
        )

    # CONFIRMACIÓN: Imprime mensaje de éxito
    print(" Archivo guardado en:")
    # Imprime la ruta completa donde se guardó el archivo
    print(filename)