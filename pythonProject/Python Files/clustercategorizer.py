import os
import json
import re
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset
from transformers import BertTokenizer, BertModel
from sklearn.model_selection import train_test_split
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
from tqdm import tqdm
import argparse
import networkx as nx
import matplotlib.pyplot as plt
from torch.utils.tensorboard import SummaryWriter


# input/output paths
input_file = "/home/gyde/Documents/sparkwiki/results/keywords_20240716_20240731.json"
lda_output_path = "/home/gyde/Documents/sparkwiki/results/lda labels/20240716_20240731_lda_labels.json"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
bert_output_path = "/home/gyde/Documents/sparkwiki/results/bert labels/20240716_20240731_bert_labels.json"
graph_file = "/home/gyde/Documents/sparkwiki/results/peaks_graph_20240716_20240731.gexf"

# Constants
TOPIC_LABELS = ["politics", "sports", "football (soccer)", "conflicts", "religion", "science", "videogames", "music", "movies", "media"]
LABEL_TO_IDX = {label: idx for idx, label in enumerate(TOPIC_LABELS)}
IDX_TO_LABEL = {idx: label for label, idx in LABEL_TO_IDX.items()}

# Initialize SummaryWriter for TensorBoard
writer = SummaryWriter(log_dir="./logs")  # Specify a directory for logs
topic_keywords = {
    "politics": [
        "election", "government", "president", "prime minister", "parliament",
        "senate", "democracy", "republican", "legislation", "campaign", "politician",
        "democrat", "referendum", "constitution", "politics", "political"
    ],
    "sports": [
        "athlete", "basketball", "tournament", "baseball", "team", "league",
        "coach", "medal", "olympic", "sport",
        "gridiron football", "american football", "hockey", "sports"
    ],
    "football (soccer)": [
        "soccer", "footballer", "football", "MLS", "FIFA", "UEFA", "World Cup",
        "Premier League", "La Liga", "striker", "midfielder", "defender",
        "goalkeeper"
    ],
    "conflicts": [
        "war", "conflict", "battle", "protest", "revolution", "insurgency",
        "terrorism", "attack", "casualties", "disaster", "shooting", "crisis",
        "occupation", "genocide", "ceasefire", "massacre", "hurricane", "conflict"
    ],
    "religion": [
        "faith", "god", "prayer", "scripture", "pope", "christ", "christianity","morman", "judaism"
        "islam", "buddhism", "religion", "jesus","evangelical",
        "holy", "worship", "saint", "theology", "spiritual",
        "sermon", "ritual", "baptism", "pilgrimage"
    ],
    "science": [
        "research", "technology", "experiment", "discovery", "hypothesis",
        "innovation", "study", "laboratory", "data", "analysis", "chemistry",
        "physics", "biology", "astronomy", "AI", "robotics", "science", "engineering"
    ],
    "videogames": [
        "videogame", "console", "RPG", "multiplayer", "online", "campaign",
        "graphics", "character", "gameplay", "esports", "gaming"
    ],
    "music": [
        "album", "singer", "band", "concert", "melody", "lyrics",
        "composition", "tour", "orchestra",
        "music", "musician", "song", "popstar"
    ],
    "movies": [
        "film", "director", "cinema", "screenplay", "actor", "actress",
        "plot", "trailer", "casting", "blockbuster", "animation", "documentary", "movie"
    ],
    "media": [
        "news", "broadcast", "journalist", "editor", "article", "headline",
        "magazine", "television", "anchor", "press", "publication", "column",
        "network", "interview", "report", "tv show"
    ]
}

# OAuth token and client credentials
ACCESS_TOKEN = None  # Initialize as None and dynamically fetch it
CLIENT_ID = "4389959d718cf0a06ad0273f0b19413a"    # Replace with your client ID
CLIENT_SECRET = "5f1cbc4995e9a673a776788fc4fac5b60cd1fa73"  # Replace with your client secret

# User-Agent string
USER_AGENT = "CapstoneProject/1.0 (https://meta.wikimedia.org; lundgyde@gmail.com)"


def clean_text(text):
    """Remove URLs, timestamps, and non-informative tokens from text."""
    if not text:
        return ""

    # Remove URLs
    text = re.sub(r"http\S+|www\.\S+|wikidata\.org\S*", "", text)

    # Remove timestamps and redundant patterns
    text = re.sub(r"\b\d{1,2}z\b|\b00t00\b|\b00\b|\b00z\b|\b00 00z\b", "", text)

    # Remove non-informative words and tokens
    text = re.sub(r"\borg\b|entity|time|calendarmodel|template|precision", "", text)

    # Remove excessive whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text




def preprocess_data(data):
    """Validate and preprocess JSON data."""
    valid_data = []
    skipped_count = 0
    for idx, item in tqdm(enumerate(data), desc="Preprocessing Data", total=len(data)):
        if not all(key in item for key in ["summary", "keywords", "properties"]):
            print(f"[Warning] Malformed node at index {idx}. Skipping...")
            skipped_count += 1
            continue

        properties = item.get("properties", {})
        if "title" not in properties:
            print(f"[Warning] Missing title for node at index {idx}. Skipping...")
            skipped_count += 1
            continue

        # Clean text fields directly here
        item["summary"] = clean_text(item.get("summary", ""))
        item["keywords"] = clean_text(" ".join(item.get("keywords", []))).split()
        item["properties"]["properties"] = [
            {
                "property": clean_text(prop["property"]),
                "values": [clean_text(str(value)) for value in prop["values"]]
            }
            for prop in properties.get("properties", [])
        ]
        properties["title"] = clean_text(properties["title"].strip().lower())
        valid_data.append(item)
    print(f"[Info] Valid data count: {len(valid_data)} | Skipped: {skipped_count}")
    return valid_data


def extract_dates_from_filename(filename):
    """Extract dates from input filename."""
    match = re.search(r"_(\d{8})_(\d{8})\.json$", filename)
    if match:
        return match.groups()
    return None, None

def train_or_load_bert(data, labels, model_path="bert_model.pth"):
    """Train a new BERT model or load an existing one."""
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    if os.path.exists(model_path):
        print(f"[Info] Loading existing BERT model from {model_path}")
        model = BertClassifier(num_classes=len(TOPIC_LABELS))
        model.load_state_dict(torch.load(model_path))
        model.to(device)
    else:
        print("[Info] Training a new BERT model...")
        model = train_bert_classifier(data, labels, save_path=model_path)

    return model



def rule_based_labeling(data):
    """Perform rule-based labeling based on article titles."""
    labels = {}
    for idx, item in enumerate(data):
        properties = item.get("properties", {})
        title = properties.get("title", "").lower()

        # Apply rules
        if "(album)" in title or "(song)" in title or "(band)" in title:
            labels[title] = "music"
        elif "(footballer)" in title or "(soccer)" in title:
            labels[title] = "football (soccer)"
        elif "(american football)" in title or "(baseball)" in title or "(basketball)" in title or "(hockey)" in title:
            labels[title] = "sports"
        elif "(film)" in title or "(actor)" in title or "(actress)" in title:
            labels[title] = "movies"
        elif "(politician)" in title or "(president)" in title or "(prime minister)" in title:
            labels[title] = "politics"
        elif "(scientist)" in title or "(researcher)" in title:
            labels[title] = "science"
        elif "(video game)" in title or "(console)" in title:
            labels[title] = "videogames"
        elif "(disaster)" in title or "(mass shooting)" in title or "(war)" in title:
            labels[title] = "conflicts"
        elif "(religion)" in title or "(church)" in title or "(temple)" in title:
            labels[title] = "religion"
        elif "(tv show)" in title or "(tv series)" in title or "(show)" in title:
            labels[title] = "media"
        else:
            labels[title] = "unlabeled"  # Default to unlabeled
    return labels

def combined_labeling(data):
    """Combine rule-based and keyword-based labels."""
    rule_labels = rule_based_labeling(data)
    keyword_labels = keyword_based_labeling(data)
    combined_labels = {}
    unlabeled_nodes = []

    for item in data:
        title = item["properties"]["title"]
        rule_label = rule_labels.get(title, "unlabeled")
        keyword_label = keyword_labels.get(title, "unlabeled")
        if rule_label != "unlabeled":
            combined_labels[title] = rule_label
        elif keyword_label != "unlabeled":
            combined_labels[title] = keyword_label
        else:
            combined_labels[title] = "unlabeled"
            unlabeled_nodes.append(item)

    print(f"[Info] Total labeled nodes: {len(combined_labels) - len(unlabeled_nodes)}")
    print(f"[Info] Unlabeled nodes: {len(unlabeled_nodes)}")
    return combined_labels, unlabeled_nodes

def label_unlabeled_nodes(graph_file, labels, n=5):
    """
    Label unlabeled nodes in the graph using the predominant label of their n most connected neighbors.

    Args:
        graph_file (str): Path to the GEXF file representing the graph structure.
        labels (dict): Dictionary of node labels.
        n (int): Number of most connected neighbors to consider.

    Returns:
        dict: Updated labels dictionary.
    """
    # Load the graph
    graph = nx.read_gexf(graph_file)

    # Count and output initial label distribution
    label_counts = {label: list(labels.values()).count(label) for label in set(labels.values())}
    print(f"[Info] Initial label distribution: {label_counts}")

    for node in graph.nodes():
        if labels.get(node, "unlabeled") == "unlabeled":
            # Get neighbors of the node and their degree
            neighbors = list(graph.neighbors(node))
            neighbors_sorted_by_degree = sorted(neighbors, key=lambda x: graph.degree(x), reverse=True)

            # Take the n most connected neighbors
            most_connected_neighbors = neighbors_sorted_by_degree[:n]
            # Collect labels of these neighbors
            neighbor_labels = [labels.get(neighbor, "unlabeled") for neighbor in most_connected_neighbors]
            # Filter out "unlabeled" from neighbor labels
            valid_labels = [label for label in neighbor_labels if label != "unlabeled"]

            if valid_labels:
                # Assign the predominant label among these neighbors
                predominant_label = max(set(valid_labels), key=valid_labels.count)
                labels[node] = predominant_label

    # Count and output final label distribution
    final_label_counts = {label: list(labels.values()).count(label) for label in set(labels.values())}
    print(f"[Info] Final label distribution: {final_label_counts}")

    return labels


def finalize_labels(graph_file_bert, graph_file_lda, bert_labels, lda_labels, n=5):
    """
    Ensure no nodes are left unlabeled. Apply neighbor-based labeling independently for BERT and LDA graphs.

    Args:
        graph_file_bert (str): Path to the BERT GEXF graph file.
        graph_file_lda (str): Path to the LDA GEXF graph file.
        bert_labels (dict): Dictionary of labels from BERT classifier.
        lda_labels (dict): Dictionary of labels from LDA topic modeling.
        n (int): Number of most connected neighbors to consider.

    Returns:
        tuple: Updated BERT and LDA labels.
    """
    print("[Info] Finalizing BERT labels...")
    bert_labels = label_unlabeled_nodes(graph_file_bert, bert_labels, n=n)

    print("[Info] Finalizing LDA labels...")
    lda_labels = label_unlabeled_nodes(graph_file_lda, lda_labels, n=n)

    return bert_labels, lda_labels


# Dataset Class
class ArticleDataset(Dataset):
    def __init__(self, summaries, keywords, properties, labels, tokenizer, max_len):
        self.summaries = summaries
        self.keywords = keywords
        self.properties = properties
        self.labels = labels
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.summaries)

    def __getitem__(self, index):
        summary = self.summaries[index]
        keywords = " ".join(self.keywords[index])
        properties = " ".join([f"{prop['property']} {' '.join(prop['values'])}" for prop in self.properties[index]])
        combined_text = f"{summary} {keywords} {properties}"

        encoding = self.tokenizer(
            combined_text,
            truncation=True,
            padding="max_length",
            max_length=self.max_len,
            return_tensors="pt"
        )
        return {
            "input_ids": encoding["input_ids"].squeeze(),
            "attention_mask": encoding["attention_mask"].squeeze(),
            "label": torch.tensor(self.labels[index], dtype=torch.long)
        }


# BERT Classifier
class BertClassifier(nn.Module):
    def __init__(self, num_classes):
        super(BertClassifier, self).__init__()
        self.bert = BertModel.from_pretrained("bert-base-uncased")
        self.dropout = nn.Dropout(0.3)
        self.classifier = nn.Linear(self.bert.config.hidden_size, num_classes)

    def forward(self, input_ids, attention_mask):
        outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        pooled_output = outputs.pooler_output
        return self.classifier(self.dropout(pooled_output))


def train_or_load_lda(combined_texts, n_topics, lda_model_path="lda_model.pkl", vectorizer_path="vectorizer.pkl"):
    """Train a new LDA model or load an existing one."""
    from joblib import load, dump

    # Check if LDA model and vectorizer files exist
    if os.path.exists(lda_model_path) and os.path.exists(vectorizer_path):
        print(f"[Info] Loading existing LDA model from {lda_model_path} and vectorizer from {vectorizer_path}")
        lda = load(lda_model_path)
        vectorizer = load(vectorizer_path)
    else:
        print("[Info] Training a new LDA model...")
        vectorizer = CountVectorizer(
            max_features=5000,
            stop_words="english",
            ngram_range=(1, 3),
            max_df=0.95,
            min_df=2
        )
        term_matrix = vectorizer.fit_transform(combined_texts)

        lda = LatentDirichletAllocation(
            n_components=n_topics,
            learning_method="online",
            learning_decay=0.7,
            max_iter=100,
            random_state=42,
            doc_topic_prior=0.01,
            topic_word_prior=0.01
        )
        lda.fit(term_matrix)

        # Save the model and vectorizer
        dump(lda, lda_model_path)
        dump(vectorizer, vectorizer_path)
        print(f"[Info] Saved LDA model to {lda_model_path} and vectorizer to {vectorizer_path}")

    return lda, vectorizer


def map_lda_to_labels(lda, vectorizer, topic_keywords, topic_labels):
    """
    Map LDA topics to predefined high-level labels.

    Args:
        lda (LatentDirichletAllocation): Trained LDA model.
        vectorizer (CountVectorizer): Vectorizer used for LDA.
        topic_keywords (dict): Predefined topic keywords.
        topic_labels (list): High-level topic labels.

    Returns:
        dict: Mapping of LDA topics to predefined labels.
    """
    # Extract LDA topic-word distributions
    feature_names = vectorizer.get_feature_names_out()
    topic_word_distributions = lda.components_ / lda.components_.sum(axis=1)[:, np.newaxis]

    lda_to_labels = {}
    for topic_idx, topic_dist in enumerate(topic_word_distributions):
        top_words = [feature_names[i] for i in topic_dist.argsort()[:-11:-1]]  # Top 10 words
        label_scores = {label: sum(1 for word in top_words if word in topic_keywords[label]) for label in topic_labels}
        # Debug the score calculation
        print(f"[Debug] Topic {topic_idx}: Top Words: {top_words}, Label Scores: {label_scores}")
        best_label = max(label_scores, key=label_scores.get)
        lda_to_labels[topic_idx] = best_label

    return lda_to_labels


def infer_lda_labels(combined_texts, lda, vectorizer, lda_to_labels, threshold=0.2):
    """
    Infer LDA topics for given texts and map them to high-level labels.

    Args:
        combined_texts (list): List of combined text inputs.
        lda (LatentDirichletAllocation): Trained LDA model.
        vectorizer (CountVectorizer): Vectorizer used for LDA.
        lda_to_labels (dict): Mapping of LDA topic indices to high-level labels.
        threshold (float): Minimum probability required to assign a label.

    Returns:
        dict: Mapping of item indices to high-level topic labels.
    """
    term_matrix = vectorizer.transform(combined_texts)
    topic_distributions = lda.transform(term_matrix)

    labels = {}
    for idx, topic_dist in enumerate(topic_distributions):
        dominant_topic = np.argmax(topic_dist)
        max_prob = topic_dist[dominant_topic]

        # Assign label only if probability exceeds threshold
        if max_prob >= threshold:
            labels[idx] = lda_to_labels.get(dominant_topic, "unlabeled")
        else:
            labels[idx] = "unlabeled"

    return labels



# Simple keyword matching for initial labeling
def keyword_based_labeling(data):
    """Label nodes based on summaries and keywords."""
    labels = {}
    for item in data:
        title = item["properties"]["title"]
        summary = item["summary"].lower()
        keywords = item["keywords"]
        matched_topics = {}  # Dictionary to store topic match counts

        # Check keywords and summary for matches with each topic
        for topic, topic_words in topic_keywords.items():
            match_count = sum(1 for word in topic_words if word in summary or word in keywords)
            if match_count > 0:
                matched_topics[topic] = match_count

        # Assign label based on the highest match count
        if matched_topics:
            assigned_topic = max(matched_topics, key=matched_topics.get)
            labels[title] = assigned_topic
        else:
            labels[title] = "unlabeled"  # Default to unlabeled if no match

    return labels


# Training and Evaluation Functions
def train_model(model, dataloader, optimizer, loss_fn, device):
    """Train the model for one epoch."""
    model.train()
    total_loss = 0
    for batch in tqdm(dataloader, desc="Training", leave=False):
        optimizer.zero_grad()
        input_ids = batch["input_ids"].to(device)
        attention_mask = batch["attention_mask"].to(device)
        labels = batch["label"].to(device)
        outputs = model(input_ids=input_ids, attention_mask=attention_mask)
        loss = loss_fn(outputs, labels)
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    avg_loss = total_loss / len(dataloader)
    return avg_loss

def evaluate_model(model, dataloader, loss_fn, device):
    """Evaluate the model on validation data."""
    model.eval()
    total_loss = 0
    correct_predictions = 0
    with torch.no_grad():
        for batch in tqdm(dataloader, desc="Evaluating", leave=False):
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["label"].to(device)
            outputs = model(input_ids, attention_mask)
            loss = loss_fn(outputs, labels)
            total_loss += loss.item()
            preds = torch.argmax(outputs, dim=1)
            correct_predictions += (preds == labels).sum().item()
    avg_loss = total_loss / len(dataloader)
    accuracy = correct_predictions / len(dataloader.dataset)
    return avg_loss, accuracy

# Automated Labeling: LDA-based topic detection
def run_wikidata_lda(data, n_topics=10):
    """Perform LDA topic modeling using Wikidata properties."""
    property_texts = []
    for item in data:
        properties = item.get("properties", {}).get("properties", [])
        prop_text = " ".join([f"{prop['property']} {' '.join(map(str, prop['values']))}" for prop in properties])
        property_texts.append(prop_text)

    vectorizer = CountVectorizer(max_features=1000, stop_words="english")
    term_matrix = vectorizer.fit_transform(property_texts)
    lda = LatentDirichletAllocation(n_components=n_topics, random_state=42)
    lda.fit(term_matrix)

    labels = {}
    for idx, text in enumerate(property_texts):
        topic_distribution = lda.transform(vectorizer.transform([text]))[0]
        dominant_topic = np.argmax(topic_distribution)
        properties = data[idx].get("properties", {})
        title = properties.get("title", f"Item_{idx}")
        labels[title] = TOPIC_LABELS[dominant_topic % len(TOPIC_LABELS)]
    return labels



def train_bert_classifier(data, initial_labels, save_path="bert_model.pth"):
    """Train a BERT classifier using labeled data."""
    labeled_data = [(item, initial_labels.get(item["properties"]["title"], "unlabeled")) for item in data]
    labeled_data = [(item, label) for item, label in labeled_data if label != "unlabeled"]

    if not labeled_data:
        print("[Error] No labeled data available for training.")
        return None

    # Prepare training data
    summaries = [item["summary"] for item, label in labeled_data]
    keywords = [item["keywords"] for item, label in labeled_data]
    properties = [item["properties"]["properties"] for item, label in labeled_data]
    label_indices = [LABEL_TO_IDX[label] for _, label in labeled_data]

    # Train-test split
    train_summaries, val_summaries, train_keywords, val_keywords, train_properties, val_properties, train_labels, val_labels = train_test_split(
        summaries, keywords, properties, label_indices, test_size=0.2, random_state=42
    )

    # Tokenization
    tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")
    train_dataset = ArticleDataset(train_summaries, train_keywords, train_properties, train_labels, tokenizer, max_len=512)
    val_dataset = ArticleDataset(val_summaries, val_keywords, val_properties, val_labels, tokenizer, max_len=512)

    # Dataloader setup
    train_loader = DataLoader(train_dataset, batch_size=16, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=16)

    # Model setup
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = BertClassifier(num_classes=len(TOPIC_LABELS)).to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=5e-5)
    loss_fn = nn.CrossEntropyLoss()

    best_val_loss = float("inf")
    patience, patience_counter = 3, 0

    # Define writer for TensorBoard
    writer = SummaryWriter(log_dir="./logs")

    print("[Info] Starting BERT training...")
    for epoch in range(20):  # Train up to 20 epochs, with early stopping
        train_loss = train_model(model, train_loader, optimizer, loss_fn, device)
        val_loss, val_acc = evaluate_model(model, val_loader, loss_fn, device)

        print(f"Epoch {epoch + 1} | Train Loss: {train_loss:.4f}, Val Loss: {val_loss:.4f}, Val Acc: {val_acc:.4f}")
        writer.add_scalars("Loss", {"Train": train_loss, "Validation": val_loss}, epoch)
        writer.add_scalar("Validation/Accuracy", val_acc, epoch)

        # Save the model periodically and on improvement
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            torch.save(model.state_dict(), save_path)
            print(f"[Info] Model improved. Saved to {save_path}")
            patience_counter = 0
        else:
            patience_counter += 1
            if patience_counter >= patience:
                print("[Info] Early stopping triggered.")
                break

    writer.close()  # Close the writer after training
    return model



def prepare_lda_input(data):
    """
    Prepare richer text data for LDA by combining and weighting summaries, keywords, and properties.

    Args:
        data (list): Preprocessed JSON data.

    Returns:
        list: Combined and weighted text for each item.
    """
    combined_texts = []
    for item in data:
        # Clean all textual fields
        summary = clean_text(item.get("summary", "").lower())
        keywords = clean_text(" ".join(item.get("keywords", [])).lower())
        properties = item.get("properties", {}).get("properties", [])

        properties_text = clean_text(" ".join([
            f"{prop['property']} {' '.join(map(str, prop['values']))}" for prop in properties
        ]))
        weighted_properties = f"{properties_text} " * 3  # Weight properties more heavily

        combined_text = " ".join(set(f"{summary} {keywords} {weighted_properties}".split())).strip()
        combined_texts.append(combined_text)

    return combined_texts

def get_args():
    """Parse command-line arguments for mode selection."""
    parser = argparse.ArgumentParser(description="Run BERT or LDA or both for labeling.")
    parser.add_argument(
        "--mode",
        choices=["bert", "lda", "both"],
        required=True,
        help="Specify whether to run BERT labeling, LDA labeling, or both."
    )
    parser.add_argument(
        "--bert_model_path",
        default="bert_model.pth",
        help="Path to save or load the BERT model (default: bert_model.pth)"
    )
    parser.add_argument(
        "--lda_model_path",
        default="lda_model.pkl",
        help="Path to save or load the LDA model (default: lda_model.pkl)"
    )
    parser.add_argument(
        "--vectorizer_path",
        default="vectorizer.pkl",
        help="Path to save or load the LDA vectorizer (default: vectorizer.pkl)"
    )
    return parser.parse_args()

# Main Method
if __name__ == "__main__":
    # Parse mode from arguments
    args = get_args()

    # Load JSON data
    with open(input_file, "r") as f:
        data = json.load(f)

    valid_data = preprocess_data(data)

    # Run BERT pipeline
    if args.mode in ["bert", "both"]:
        print("[Info] Running BERT pipeline...")
        combined_labels, unlabeled_nodes = combined_labeling(valid_data)

        # Load or train the BERT model
        bert_model = train_or_load_bert(valid_data, combined_labels, model_path=args.bert_model_path)

        bert_labels = {node["properties"]["title"]: combined_labels.get(node["properties"]["title"], "unlabeled")
                       for node in valid_data}
        bert_labels = label_unlabeled_nodes(graph_file, bert_labels, n=5)
        with open(bert_output_path, "w") as f:
            json.dump(bert_labels, f, indent=4)

    # Run LDA pipeline
    if args.mode in ["lda", "both"]:
        print("[Info] Running LDA pipeline...")
        combined_texts = prepare_lda_input(valid_data)

        # Load or train the LDA model
        lda, vectorizer = train_or_load_lda(
            combined_texts,
            n_topics=len(TOPIC_LABELS),
            lda_model_path=args.lda_model_path,
            vectorizer_path=args.vectorizer_path
        )

    # Map topics and infer labels
        lda_to_labels = map_lda_to_labels(lda, vectorizer, topic_keywords, TOPIC_LABELS)
        lda_labels = infer_lda_labels(combined_texts, lda, vectorizer, lda_to_labels, threshold=0.25)

        lda_title_labels = {
            node["properties"]["title"]: lda_labels.get(idx, "unlabeled") for idx, node in enumerate(valid_data)
            if "title" in node["properties"]
        }

        # Count and log label distribution
        label_distribution = {label: list(lda_title_labels.values()).count(label) for label in set(lda_title_labels.values())}
        print(f"[Info] Label distribution: {label_distribution}")

        # Save results
        with open(lda_output_path, "w") as f:
            json.dump(lda_title_labels, f, indent=4)

    print("[Info] Processing completed.")
