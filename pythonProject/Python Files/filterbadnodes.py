import pandas as pd
import glob
import os

# Paths to original and filtered directories
normal_pages_paths = "/home/gyde/Documents/sparkwiki/outputs/page/normal_pages/*.csv.gz"
category_pages_paths = "/home/gyde/Documents/sparkwiki/outputs/page/category_pages/*.csv.gz"
pagelinks_paths = "/home/gyde/Documents/sparkwiki/outputs/pagelinks/*.csv.gz"
categorylinks_paths = "/home/gyde/Documents/sparkwiki/outputs/categorylinks/*.csv.gz"

# Paths to save filtered files
filtered_dir = {
    "normal_pages": "/home/gyde/Documents/sparkwiki/outputs/page/filt_normal_pages/",
    "category_pages": "/home/gyde/Documents/sparkwiki/outputs/page/filt_category_pages/",
    "pagelinks": "/home/gyde/Documents/sparkwiki/outputs/filt_pagelinks/",
    "categorylinks": "/home/gyde/Documents/sparkwiki/outputs/filt_categorylinks/"
}

# Create filtered directories if they don’t exist
for path in filtered_dir.values():
    os.makedirs(path, exist_ok=True)

# Define column names based on headers
normal_pages_columns = ["id:ID", "title", "isRedirect:BOOLEAN", "isNew:BOOLEAN"]
category_pages_columns = ["id:ID", "title", "isRedirect:BOOLEAN", "isNew:BOOLEAN"]
pagelinks_columns = [":START_ID", ":END_ID"]
categorylinks_columns = [":START_ID", "title", ":END_ID", "ctype"]

# Load and combine node data from compressed files without headers
normal_pages = pd.concat([
    pd.read_csv(file, delimiter='\t', header=None, names=normal_pages_columns, compression='gzip')
    for file in glob.glob(normal_pages_paths)
])
category_pages = pd.concat([
    pd.read_csv(file, delimiter='\t', header=None, names=category_pages_columns, compression='gzip')
    for file in glob.glob(category_pages_paths)
])

# Combine all node IDs into a set for quick lookup
all_nodes = set(pd.concat([normal_pages["id:ID"], category_pages["id:ID"]]))

# Load, filter, and save pagelinks without headers
for i, file in enumerate(glob.glob(pagelinks_paths)):
    pagelinks = pd.read_csv(file, delimiter='\t', header=None, names=pagelinks_columns, compression='gzip')
    filtered_pagelinks = pagelinks[pagelinks[":START_ID"].isin(all_nodes) & pagelinks[":END_ID"].isin(all_nodes)]
    filtered_pagelinks.to_csv(f"{filtered_dir['pagelinks']}filtered_pagelinks_{i}.csv.gz", sep='\t', index=False, header=False, compression='gzip')

# Load, filter, and save categorylinks without headers
for i, file in enumerate(glob.glob(categorylinks_paths)):
    categorylinks = pd.read_csv(file, delimiter='\t', header=None, names=categorylinks_columns, compression='gzip')
    filtered_categorylinks = categorylinks[
        categorylinks[":START_ID"].isin(all_nodes) & categorylinks[":END_ID"].isin(all_nodes)
        ]
    filtered_categorylinks.to_csv(f"{filtered_dir['categorylinks']}filtered_categorylinks_{i}.csv.gz", sep='\t', index=False, header=False, compression='gzip')

# Filter and save normal and category pages by valid IDs
for i, file in enumerate(glob.glob(normal_pages_paths)):
    normal_page = pd.read_csv(file, delimiter='\t', header=None, names=normal_pages_columns, compression='gzip')
    filtered_normal_page = normal_page[normal_page["id:ID"].isin(all_nodes)]
    filtered_normal_page.to_csv(f"{filtered_dir['normal_pages']}filtered_normal_pages_{i}.csv.gz", sep='\t', index=False, header=False, compression='gzip')

for i, file in enumerate(glob.glob(category_pages_paths)):
    category_page = pd.read_csv(file, delimiter='\t', header=None, names=category_pages_columns, compression='gzip')
    filtered_category_page = category_page[category_page["id:ID"].isin(all_nodes)]
    filtered_category_page.to_csv(f"{filtered_dir['category_pages']}filtered_category_pages_{i}.csv.gz", sep='\t', index=False, header=False, compression='gzip')

print("Filtering complete. Files saved to respective 'filt_' directories without headers.")
