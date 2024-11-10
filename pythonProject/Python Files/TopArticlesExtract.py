import requests as rq
import pandas as pd
import matplotlib.pyplot as plt

# Set the headers
headers = {
    "User-Agent": "Wikimedia Analytics API Tutorial (<your wiki username>) top-viewed-articles.py",
}

# Define the endpoint URL
top_articles_url = """https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/2024/06/all-days"""

# Make the request
top_articles_response = rq.get(top_articles_url, headers=headers).json()

# Extract the items and convert to a DataFrame
articles_df = pd.DataFrame(top_articles_response['items'][0]['articles'])

# Display the top 100 articles based on views
top_100_articles = articles_df[['article', 'views']].head(100)

# Plot the data
plt.figure(figsize=(10, 6))
plt.barh(top_100_articles['article'], top_100_articles['views'], color='skyblue')
plt.xlabel('Views')
plt.ylabel('Article')
plt.title('Top 10 Viewed Articles - June 2024 (English Wikipedia)')
plt.gca().invert_yaxis()
plt.show()
