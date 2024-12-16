import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Specify the base URL for Wikimedia dumps
BASE_URL = "https://dumps.wikimedia.org/other/pageview_complete/2024/"

# List of months to download
months = ["2024-05", "2024-07", "2024-08"]

# Output folder to store downloaded files
OUTPUT_FOLDER = "/home/gyde/Documents/bzsets-pageviews/all_views"  # Update this with your desired directory

# Ensure the output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def get_file_links(month_url):
    """Fetch all 'user.bz2' file links for a specific month."""
    response = requests.get(month_url)
    if response.status_code != 200:
        print(f"Failed to fetch {month_url}")
        return []
    soup = BeautifulSoup(response.content, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True) if "user.bz2" in a["href"]]
    return [month_url + link for link in links]

def download_file(url, output_path):
    """Download a file from a URL and save it """
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        total_size = int(response.headers.get("content-length", 0))
        with open(output_path, "wb") as file, tqdm(
                desc=os.path.basename(output_path),
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
        ) as bar:
            for data in response.iter_content(chunk_size=1024):
                file.write(data)
                bar.update(len(data))
    else:
        print(f"Failed to download {url}")

# Main process
for month in months:
    print(f"Processing {month}...")
    month_url = BASE_URL + f"{month}/"
    file_links = get_file_links(month_url)
    if not file_links:
        print(f"No 'user.bz2' files found for {month}.")
        continue
    for file_link in file_links:
        file_name = os.path.basename(file_link)
        output_path = os.path.join(OUTPUT_FOLDER, file_name)
        if not os.path.exists(output_path):  # Skip if already downloaded
            print(f"Downloading {file_name}...")
            download_file(file_link, output_path)
        else:
            print(f"File {file_name} already exists. Skipping.")
