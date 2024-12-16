import os
import requests
import json
import networkx as nx
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from urllib.parse import quote
import time
import nltk
import re

# Ensure necessary NLTK components are downloaded
nltk.download("punkt")
nltk.download("stopwords")

# API Endpoints
WIKIPEDIA_API = "https://en.wikipedia.org/api/rest_v1/page/summary"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
CACHE_FILE = "/home/gyde/Documents/sparkwiki/results/translation_cache.json"

# OAuth token and client credentials
ACCESS_TOKEN = None  # Initialize as None and dynamically fetch it
CLIENT_ID = "4389959d718cf0a06ad0273f0b19413a"    # Replace with your client ID
CLIENT_SECRET = "5f1cbc4995e9a673a776788fc4fac5b60cd1fa73"  # Replace with your client secret
USER_AGENT = "CapstoneProject/1.0 (https://meta.wikimedia.org; lundgyde@gmail.com)"

def get_headers():
    """Return headers with a valid access token and user-agent."""
    return {
        "Authorization": f"Bearer {get_or_refresh_access_token()}",
        "User-Agent": USER_AGENT
    }

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=4)
        print(f"Cache saved to {CACHE_FILE}")


def clean_text(text):
    if not text:
        return ""
    text = re.sub(r"http\S+|www\.\S+|wikidata\.org\S*", "", text)
    text = re.sub(r"\b\d{1,2}z\b|\b00t00\b|\b00\b|\b00z\b|\b00 00z\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def get_or_refresh_access_token():
    """Return a valid access token, refreshing it if necessary."""
    global ACCESS_TOKEN
    if not ACCESS_TOKEN:
        refresh_access_token()
    return ACCESS_TOKEN

def fetch_wikipedia_summary(page_title):
    """Fetch the summary, description, and Wikibase ID from a Wikipedia page."""
    try:
        formatted_title = quote(page_title.replace(" ", "_"))
        response = requests.get(
            f"{WIKIPEDIA_API}/{formatted_title}",
            headers={"User-Agent": USER_AGENT}
        )
        if response.status_code == 200:
            data = response.json()
            return data.get("extract", ""), data.get("description", ""), data.get("wikibase_item", None)
        else:
            print(f"Failed to fetch summary for page {page_title}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching Wikipedia summary for {page_title}: {e}")
    return "", "", None

def refresh_access_token():
    """Refresh the OAuth token if it has expired."""
    try:
        response = requests.post(
            "https://www.wikidata.org/w/rest.php/oauth2/access_token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            }
        )
        if response.status_code == 200:
            token_data = response.json()
            new_token = token_data.get("access_token", "")
            if new_token:
                global ACCESS_TOKEN
                ACCESS_TOKEN = new_token
                print("Access token refreshed successfully.")
                return True
            else:
                print("Failed to fetch a new access token.")
        else:
            print(f"Failed to refresh access token: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error refreshing access token: {e}")
    return False

def fetch_labels_in_batch(ids, type_, cache, language="en"):
    headers = get_headers()
    resolved_labels = {}
    ids_to_query = [id_ for id_ in ids if id_ not in cache]

    for i in range(0, len(ids_to_query), 50):
        batch = ids_to_query[i:i + 50]
        params = {
            "action": "wbgetentities",
            "ids": "|".join(batch),
            "format": "json",
            "props": "labels",
            "languages": language
        }
        try:
            response = requests.get(WIKIDATA_API, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                for entity_id in batch:
                    label = (
                        data.get("entities", {})
                        .get(entity_id, {})
                        .get("labels", {})
                        .get(language, {})
                        .get("value", entity_id)
                    )
                    cache[entity_id] = clean_text(label)
            else:
                print(f"[Error] Failed batch request: {response.status_code}")
        except Exception as e:
            print(f"[Error] Exception during batch fetch: {e}")

    return {id_: cache.get(id_, id_) for id_ in ids}

def fetch_wikidata_properties(wikidata_id, language="en"):
    """
    Fetch all properties for a Wikidata entity without resolving relationships.
    """
    if not wikidata_id:
        return {}

    headers = get_headers()
    params = {
        "action": "wbgetentities",
        "ids": wikidata_id,
        "format": "json",
        "props": "claims|labels|descriptions",
        "languages": language
    }

    try:
        response = requests.get(WIKIDATA_API, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            entity = data.get("entities", {}).get(wikidata_id, {})
            claims = entity.get("claims", {})
            label = entity.get("labels", {}).get(language, {}).get("value", wikidata_id)
            description = entity.get("descriptions", {}).get(language, {}).get("value", "")
            properties = process_relevant_properties(claims)

            # Return simplified structure
            return {
                "title": label,
                "description": description,
                "properties": properties
            }
        elif response.status_code == 403:
            print(f"Authorization error: Retrying with refreshed token.")
            if refresh_access_token():
                return fetch_wikidata_properties(wikidata_id, language)  # Retry with new token
        elif response.status_code == 404:
            print(f"Resource not found for Wikidata ID {wikidata_id}")
        else:
            print(f"Failed to fetch Wikidata properties for ID {wikidata_id}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching Wikidata properties for ID {wikidata_id}: {e}")
    return {}



def fetch_relevant_properties():
    """Fetch dynamically relevant properties."""
    params = {
        "action": "wbsearchentities",
        "search": "labeling",
        "type": "property",
        "format": "json",
        "limit": 50
    }
    response = requests.get(WIKIDATA_API, params=params)
    if response.status_code == 200:
        return [prop["id"] for prop in response.json().get("search", [])]
    return []

RELEVANT_PROPERTIES = fetch_relevant_properties()


def process_relevant_properties(claims):
    """
    Simplified processing of Wikidata claims. Collects all properties and raw values.
    """
    all_properties = []

    for prop, values in claims.items():
        resolved_values = []

        for value in values:
            mainsnak = value.get("mainsnak", {})
            datatype = mainsnak.get("datatype")

            # Collect raw data for each property without recursion
            if datatype == "wikibase-item":
                value_id = mainsnak.get("datavalue", {}).get("value", {}).get("id")
                if value_id:
                    resolved_values.append(value_id)  # Collect IDs for further processing

            elif datatype in ["string", "quantity", "time"]:
                resolved_values.append(mainsnak.get("datavalue", {}).get("value", ""))

        if resolved_values:
            all_properties.append({"property": prop, "values": resolved_values})

    return all_properties

def translate_wikidata_properties(data, cache):
    """
    Translate Wikidata property and item IDs in the JSON data to human-readable text using caching.

    Args:
        data (list): List of node data dictionaries with Wikidata properties.
        cache (dict): Cache for resolved labels.

    Returns:
        list: Data with translated properties.
    """
    property_ids, item_ids = set(), set()

    # Collect property and item IDs
    for item in data:
        if not isinstance(item, dict):  # Skip if item is not a dictionary
            continue

        properties = item.get("properties", {}).get("properties", [])
        for prop in properties:
            if isinstance(prop, dict) and prop.get("property", "").startswith("P"):
                property_ids.add(prop["property"])
                for value in prop.get("values", []):
                    if isinstance(value, str) and value.startswith("Q"):
                        item_ids.add(value)

    # Resolve property and item IDs
    resolved_properties = fetch_labels_in_batch(list(property_ids), "property", cache)
    resolved_items = fetch_labels_in_batch(list(item_ids), "item", cache)

    # Translate properties
    for item in data:
        if not isinstance(item, dict):  # Skip if item is not a dictionary
            continue

        properties = item.get("properties", {}).get("properties", [])
        for prop in properties:
            if isinstance(prop, dict):
                prop["property"] = resolved_properties.get(prop.get("property"), prop.get("property"))
                prop["values"] = [
                    resolved_items.get(val, val) if isinstance(val, str) and val.startswith("Q") else val
                    for val in prop.get("values", [])
                ]

    return data



def resolve_property_labels(property_keys, headers, language="en"):
    """
    Resolve property keys (e.g., P31, P279) into human-readable labels using a batch process.
    """
    if not property_keys:
        return []

    try:
        # Limit batch size to 50 to conform to API restrictions
        resolved_labels = {}
        for i in range(0, len(property_keys), 50):
            batch = property_keys[i:i + 50]
            params = {
                "action": "wbgetentities",
                "ids": "|".join(batch),
                "format": "json",
                "props": "labels",
                "languages": language
            }
            response = requests.get(WIKIDATA_API, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                resolved_labels.update({
                    key: data["entities"].get(key, {}).get("labels", {}).get(language, {}).get("value", key)
                    for key in batch
                })
        return [resolved_labels.get(key, key) for key in property_keys]
    except Exception as e:
        print(f"Error resolving property labels: {e}")
    return []


def log_progress(node_count, start_time):
    """Log progress after processing a batch of nodes."""
    elapsed_time = time.time() - start_time
    print(f"[Progress] Processed {node_count} nodes in {elapsed_time:.2f} seconds.")


def extract_qualifiers(qualifiers, headers, language):
    """Extract human-readable qualifiers for a property."""
    qualifier_data = {}
    for prop, values in qualifiers.items():
        resolved_prop = resolve_property_labels([prop], headers, language)[0]
        resolved_values = [resolve_property_labels([value.get("datavalue", {}).get("value", {}).get("id")], headers, language)[0]
                           for value in values if value.get("datavalue", {}).get("value")]
        qualifier_data[resolved_prop] = resolved_values
    return qualifier_data

def extract_references(references, headers, language):
    """Extract human-readable references for a property."""
    reference_data = []
    for ref_group in references:
        ref_values = {}
        for prop, ref_claims in ref_group.get("snaks", {}).items():
            resolved_prop = resolve_property_labels([prop], headers, language)[0]
            ref_values[resolved_prop] = [claim.get("datavalue", {}).get("value") for claim in ref_claims]
        reference_data.append(ref_values)
    return reference_data

def batch_fetch_entities(entity_ids, headers, language="en"):
    if not entity_ids:
        return {}

    try:
        params = {
            "action": "wbgetentities",
            "ids": "|".join(entity_ids[:50]),  # Query up to 50 IDs
            "format": "json",
            "props": "claims|labels|descriptions",
            "languages": language
        }
        response = requests.get(WIKIDATA_API, headers=headers, params=params)
        if response.status_code == 200:
            return response.json().get("entities", {})
        else:
            print(f"Failed batch fetch for entities: {response.status_code}")
    except Exception as e:
        print(f"Error during batch fetch: {e}")
    return {}



def process_claims(claims, headers, language, traverse_hierarchy=True):
    """Process claims and extract resolved properties."""
    properties = []
    relevant_properties = [
        "P31", "P279", "P361", "P1269", "P910", "P373",
        "P6186", "P136", "P921", "P642", "P585", "P276",
        "P129", "P3613"
    ]

    for prop, values in claims.items():
        # Check if property is relevant
        if prop not in relevant_properties:
            continue

        # Resolve property label
        resolved_prop = resolve_property_labels([prop], headers, language)[0]
        resolved_values = []

        for value in values:
            mainsnak = value.get("mainsnak", {})
            datatype = mainsnak.get("datatype")

            if datatype == "wikibase-item":
                # Extract ID for linked item
                value_id = mainsnak.get("datavalue", {}).get("value", {}).get("id")
                if value_id:
                    # Resolve label for the linked item
                    resolved_value = resolve_property_labels([value_id], headers, language)[0]
                    resolved_values.append(resolved_value)

                    # Optionally traverse hierarchy for subclasses or parts
                    if prop in ["P279", "P361"] and traverse_hierarchy:
                        nested_properties = fetch_wikidata_properties(value_id, traverse_hierarchy=False).get("properties", [])
                        resolved_values.extend(nested_properties)

            elif datatype == "string":
                # Handle string datatypes
                resolved_values.append(mainsnak.get("datavalue", {}).get("value", ""))

            elif datatype == "time":
                # Handle time datatypes
                resolved_values.append(mainsnak.get("datavalue", {}).get("value", {}).get("time", ""))

            elif datatype == "quantity":
                # Handle quantities
                resolved_values.append(mainsnak.get("datavalue", {}).get("value", {}).get("amount", ""))

        if resolved_values:
            properties.append({"property": resolved_prop, "values": resolved_values})
    return properties


def preprocess_text(text):
    """Tokenize and preprocess text, removing stop words."""
    if not text:
        return ""
    try:
        tokens = word_tokenize(text)
        stop_words = set(stopwords.words("english"))
        filtered = [
            word.lower()
            for word in tokens
            if word.lower() not in stop_words
        ]
        return " ".join(filtered)
    except Exception as e:
        print(f"Error during text preprocessing: {e}")
        return ""

def extract_keywords_with_tfidf(graph, top_k=10):
    """Extract keywords using TF-IDF weighted by node degree."""
    node_data = []  # Use a list to store node data
    tfidf_documents = []
    start_time = time.time()
    node_count = 0

    for node, data in graph.nodes(data=True):
        node_count += 1

        title = data.get("label")  # Adjust to match the GEXF file attribute for titles
        if not title:
            print(f"[Node {node_count}] Title not found for node {node}, skipping.")
            continue

        degree = graph.degree[node]

        # Fetch summaries, descriptions, and Wikibase ID
        summary, description, wikibase_item = fetch_wikipedia_summary(title)
        processed_text = preprocess_text(f"{summary} {description}")

        if processed_text:
            node_entry = {
                "title": title,
                "summary": processed_text,
                "keywords": [],
                "properties": fetch_wikidata_properties(wikibase_item),  # Store resolved properties
                "degree": degree,
            }
            node_data.append(node_entry)  # Append the node entry to the list
            tfidf_documents.append(processed_text)

        # Log progress every 100 nodes
        if node_count % 100 == 0:
            elapsed_time = time.time() - start_time
            print(f"[Progress] Processed {node_count} nodes in {elapsed_time:.2f} seconds.")

    # Compute TF-IDF scores
    try:
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(tfidf_documents)
        feature_names = vectorizer.get_feature_names_out()

        # Extract top keywords
        for idx, entry in enumerate(node_data):
            row = tfidf_matrix[idx].toarray()[0]
            top_indices = row.argsort()[-top_k:]
            entry["keywords"] = [feature_names[i] for i in top_indices]

        print(f"[Complete] Processed all nodes in {time.time() - start_time:.2f} seconds.")
        return node_data  # Return as a list
    except Exception as e:
        print(f"Error during TF-IDF computation: {e}")
        return []


def save_keywords_and_summaries(node_data, output_path):
    """Save extracted node data to a JSON file."""
    try:
        with open(output_path, "w") as f:
            json.dump(node_data, f, indent=4)
        print(f"Data saved to {output_path}")
    except Exception as e:
        print(f"Error saving data to {output_path}: {e}")

if __name__ == "__main__":
    # Input GEXF file path
    gexf_file = "/home/gyde/Documents/sparkwiki/results/graphs/peaks_graph_20240716_20240731.gexf"
    output_file = "/home/gyde/Documents/sparkwiki/results/keywords_20240716_20240731.json"

    # Load cache
    cache = load_cache()

    # Read the GEXF file
    try:
        graph = nx.read_gexf(gexf_file)
    except Exception as e:
        print(f"Error reading GEXF file {gexf_file}: {e}")
        exit(1)

    # Extract data
    print("Extracting data...")
    node_data = extract_keywords_with_tfidf(graph)

    # Translate Wikidata properties and save cache
    print("Translating properties...")
    node_data = translate_wikidata_properties(node_data, cache)
    save_cache(cache)

    # Save the extracted data
    save_keywords_and_summaries(node_data, output_file)