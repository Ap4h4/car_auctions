import requests
import csv
from collections import defaultdict
import logging
import datetime

logger = logging.getLogger(__name__)

SPARQL_URL = "https://query.wikidata.org/sparql"

QUERY = """
SELECT ?manufacturerLabel ?modelLabel WHERE {
  ?model wdt:P31 wd:Q3231690 .
  ?model wdt:P176 ?manufacturer .
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en".
  }
}
"""

HEADERS = {
    "Accept": "application/sparql-results+json",
    "User-Agent": "car-extractor/1.0"
}

def fetch_wikidata():
    logger.info("Connecting to the wikidata "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    response = requests.get(
        SPARQL_URL,
        params={"query": QUERY,"format": "json"},
        headers=HEADERS,
        timeout=60
    )
    response.raise_for_status()
    print(response.raise_for_status())
    logger.info("Getting response from wikidata "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info(f"Status of wikidata response: {response.status_code}")
    logger.info(f"Wikidata Content-Type: {response.headers.get('Content-Type')}")
    return response.json()["results"]["bindings"]

def normalize(text):
    return text.upper().strip()

def build_dictionary(rows):
    cars = defaultdict(set)

    for row in rows:
        brand = normalize(row["manufacturerLabel"]["value"])
        model = normalize(row["modelLabel"]["value"])

        # prosta sanity-check heurystyka
        if len(brand) < 2 or len(model) < 2:
            continue

        cars[brand].add(model)
    #sorting alpahbetically by manaufacturer
    sorted_cars = dict(sorted(cars.items(), key=lambda x: x[0]))
    
    
    return sorted_cars