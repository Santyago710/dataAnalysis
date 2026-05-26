# ╔═══════════════════════════════════════════════════════════════╗
# ║           IMPORTS - Required Libraries                        ║
# ╚═══════════════════════════════════════════════════════════════╝

import requests  # Library to perform HTTP requests (GET, POST, etc.)
import json  # Library to work with and convert data in JSON format
from datetime import datetime  # To get the current system date and time
from pathlib import Path  # To handle file paths in a cross-platform way

# ─── Configuration ─────────────────────────────

# REDDIT API URL
# r/Colombia: accesses the specific Colombia subreddit
# new: retrieves the most recent posts
# limit=100: limits the response to a maximum of 100 posts
url = "https://api.reddit.com/r/Colombia/new?limit=100"

# HTTP HEADERS - Information sent to the server
# User-Agent: simulates a request coming from a real browser (Mozilla Firefox)
# Without this, Reddit may reject the request for being a bot/script
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# KEYWORDS - Political terms used to filter relevant posts
# List of words to search for in titles and post content
keywords = [
    "petro", "uribe", "duque",              # Names of Colombian politicians
    "gobierno", "presidente",               # Government-related terms
    "política", "politica",                 # Main topic (with and without accent)
    "elecciones", "votar",                  # Electoral processes
    "congreso", "senado",                   # Political institutions
    "reforma", "corrupción",                # Changes and political issues
    "corrupcion", "estado",                 # Variants without accent and state power
    "ley", "ministro"                       # Legislation and political roles
]

# ─── HTTP Request to Reddit ─────────────────────────────

# Perform a GET request to the Reddit URL
# response: object containing server response (status code, headers, content)
response = requests.get(url, headers=headers)

# VALIDATION 1: Check if the request was successful
# status_code 200 = OK (successful request)
# status_code 404 = Not found
# status_code 403 = Forbidden
# status_code 429 = Too many requests (temporarily blocked)
if response.status_code != 200:
    # If the request fails, print the error code
    print("❌ Request error:", response.status_code)
    # Show the first 300 characters of the response for debugging
    print(response.text[:300])
    # exit() completely terminates the program
    exit()

# VALIDATION 2: Attempt to convert the response to JSON format
# If the response is not valid JSON, this will raise an exception
try:
    # response.json() converts JSON text into Python dictionaries and lists
    data = response.json()
except Exception as e:
    # If there is an error in conversion, catch the exception
    print("❌ Error converting to JSON:", e)
    # Show part of the problematic response
    print(response.text[:300])
    # Terminate the program
    exit()

# EXTRACTION of posts from JSON structure
# Reddit returns: {"data": {"children": [{"data": {...}}, {"data": {...}}]}}
# .get("data", {}) gets the "data" field, or {} if it doesn't exist
# .get("children", []) gets "children" from data, or [] if it doesn't exist
# Result: a list containing all retrieved posts
posts = data.get("data", {}).get("children", [])

# INITIALIZATION: Empty list to store filtered (political) posts
filtered_posts = []

# ─── Post Filtering ─────────────────────────────

# LOOP: Iterates over each retrieved post
for post in posts:
    # Extract the "data" dictionary containing post information
    # If "data" does not exist, use an empty dictionary {}
    data_post = post.get("data", {})

    # FILTER 1: Ignore "stickied" (pinned) posts
    # Stickied posts are usually announcements or moderator messages
    # data_post.get("pinned"): returns True/False, or None if not present
    if data_post.get("pinned"):
        # continue: skip this post and move to the next iteration
        continue

    # TEXT EXTRACTION: Get title and content to search for keywords
    # .lower(): converts to lowercase for case-insensitive comparison
    # Example: "POLÍTICA" and "política" will be treated the same
    title = data_post.get("title", "").lower()
    text = data_post.get("selftext", "").lower()

    # COMBINATION: Merge title and content for a more complete search
    # A space is added to prevent words from merging
    contenido = title + " " + text

    # FILTER 2: Skip empty or very short posts (less than 20 characters)
    # .strip(): removes leading and trailing whitespace
    # len(): counts number of characters
    if len(contenido.strip()) < 20:
        # Skip very short posts that are likely not relevant
        continue

    # FILTER 3: Check if ANY keyword exists in the content
    # any(): returns True if AT LEAST ONE condition is True
    # (k in contenido for k in keywords): checks each keyword
    # Example: if "petro" in contenido or "uribe" in contenido or ...
    if any(k in contenido for k in keywords):
        # If at least one keyword is found, add the post to filtered list
        # Create a dictionary with the main post data
        filtered_posts.append({
            "title": data_post.get("title", ""),              # Original title (not lowercased)
            "text": data_post.get("selftext", ""),            # Post body (if text-based)
            "author": data_post.get("author", ""),            # Username of the author
            "date": data_post.get("created_utc", ""),         # Creation date (Unix timestamp)
            "score": data_post.get("score", 0),               # Score (upvotes - downvotes)
            # Build the full Reddit post URL
            "url": "https://reddit.com" + data_post.get("permalink", "")
        })

# ─── Statistics ─────────────────────────────

# Print total number of retrieved posts (max 100)
print(" Total posts:", len(posts))
# Print how many posts matched political keywords
print(" Political posts:", len(filtered_posts))

# ─── Output Path ─────────────────────────────

# PATH DEFINITION: Defines where JSON files will be saved
# Path(): creates a path object that works on Windows, Linux, and Mac
# r"...": raw string - treats backslashes as literal characters
# This is the folder where all downloaded files will be stored
output_path = Path(__file__).resolve().parent.parent / "datalake_bronze/dataAPI" 

# DIRECTORY CREATION: Create the folder if it does not exist
# parents=True: creates parent directories if needed
# exist_ok=True: prevents errors if the folder already exists
output_path.mkdir(parents=True, exist_ok=True)

# TIMESTAMP GENERATION: Creates a unique filename using current date and time
# datetime.now(): gets current system date and time
# strftime(): formats the date according to the given pattern
# "%Y%m%d_%H%M%S" = YEAR MONTH DAY_HOUR MINUTE SECOND
# Example: "20260321_143045" = March 21, 2026 at 14:30:45
fecha = datetime.now().strftime("%Y%m%d_%H%M%S")

# FILENAME CONSTRUCTION
# output_path / f"...": Path operator to concatenate paths
# f"...": f-string to embed variables
# Result: "...DataAPI\reddit_colombia_20260321_143045.json"
filename = output_path / f"reddit_colombia_{fecha}.json"

# ─── Save JSON ─────────────────────────────

# CONDITION: Check if any political posts were found
if len(filtered_posts) == 0:
    # If no posts found, show warning and do not create file
    print("⚠️ No political posts found")
else:
    # If posts exist, proceed to save them
    
    # FILE OPENING: Open file in write mode
    # with open(): context manager that automatically closes the file
    # "w": write mode (overwrites if file exists)
    # encoding="utf-8": supports special characters (ñ, accents, emojis)
    with open(filename, "w", encoding="utf-8") as f:
        # JSON SERIALIZATION: Convert list to JSON and write to file
        json.dump(
            filtered_posts[:20],        # Take only first 20 posts (limit size)
            f,                          # File to write into
            indent=4,                   # Pretty format with 4 spaces
            ensure_ascii=False          # Preserve Latin characters
        )

    # CONFIRMATION: Print success message
    print(" File saved at:")
    # Print full path where file was saved
    print(filename)