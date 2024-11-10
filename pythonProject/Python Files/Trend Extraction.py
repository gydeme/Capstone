import requests as rq
import pandas as pd
from datetime import datetime, timedelta

# Define headers and time period for API queries
headers = {"User-Agent": "Wikimedia Analytics API Tutorial (<your wiki username>)"}
language = "en.wikipedia"
project = "all-access"
start_date = "20241001"
end_date = "20241031"
lookback_period_days = 30
burst_threshold_std_dev = 2

# Function to fetch historical page views for baseline calculation
def fetch_pageviews(article, start_date, end_date):
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{language}/{project}/all-agents/{article}/daily/{start_date}/{end_date}"
    response = rq.get(url, headers=headers).json()
    if 'items' in response:
        return pd.DataFrame(response['items']).set_index('timestamp')['views'].astype(int)
    return pd.Series()

# Function to calculate mean and std dev of historical views
def calculate_baseline(article, lookback_days):
    end_date = datetime.now()
    start_date = (end_date - timedelta(days=lookback_days)).strftime('%Y%m%d')
    views = fetch_pageviews(article, start_date, end_date.strftime('%Y%m%d'))
    if not views.empty:
        return views.mean(), views.std()
    return None, None

# Detect trending articles based on burst criteria
def detect_trending_articles(articles, current_period, threshold=burst_threshold_std_dev):
    trending_articles = []
    for article in articles:
        mean_views, std_views = calculate_baseline(article, lookback_period_days)
        if mean_views and std_views:
            # Fetch current period views and detect bursts
            current_views = fetch_pageviews(article, *current_period)
            if current_views.mean() > mean_views + threshold * std_views:
                trending_articles.append(article)
    return trending_articles

# Main execution to detect bursts and form clusters
article_list = ["Example_Article_1", "Example_Article_2"]  # Sample articles
current_period = ("20241001", "20241031")  # Example month
trending_articles = detect_trending_articles(article_list, current_period)

# Function to fetch hyperlinks from a Wikipedia article
def fetch_article_links(article):
    url = f"https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "links",
        "titles": article,
        "format": "json",
        "pllimit": "max"  # Set to max to get as many links as possible per request
    }

    links = []
    while True:
        response = rq.get(url, params=params).json()
        pages = response.get("query", {}).get("pages", {})
        for page_id, page_data in pages.items():
            if "links" in page_data:
                links.extend([link["title"] for link in page_data["links"]])

        # Continue fetching if there is more data available (pagination)
        if 'continue' in response:
            params.update(response['continue'])
        else:
            break

    return links


# Cluster formation (based on linked articles with bursts)
clusters = {}
for article in trending_articles:
    linked_articles = fetch_article_links(article)  # Use previous function for hyperlinks
    burst_links = [link for link in linked_articles if link in trending_articles]
    clusters[article] = burst_links

# Display clusters
print("Trending Article Clusters:")
for article, links in clusters.items():
    print(f"{article}: {links}")
