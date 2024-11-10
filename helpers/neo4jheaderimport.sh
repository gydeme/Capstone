#!/bin/sh
delim="\t"
data_dir="/home/gyde/Documents/sparkwiki/outputs"

# Collect the actual file paths for each type of data
normal_pages=$(ls $data_dir/page/normal_pages/part-*.csv.gz | tr '\n' ',')
category_pages=$(ls $data_dir/page/category_pages/part-*.csv.gz | tr '\n' ',')
pagelinks=$(ls $data_dir/pagelinks/part-*.csv.gz | tr '\n' ',')
categorylinks=$(ls $data_dir/categorylinks/part-*.csv.gz | tr '\n' ',')

# Remove trailing commas from the lists
normal_pages=${normal_pages%,}
category_pages=${category_pages%,}
pagelinks=${pagelinks%,}
categorylinks=${categorylinks%,}

# Check if any file list is empty and report missing files
if [ -z "$normal_pages" ]; then echo "Normal pages CSV files not found."; exit 1; fi
if [ -z "$category_pages" ]; then echo "Category pages CSV files not found."; exit 1; fi
if [ -z "$pagelinks" ]; then echo "PageLinks CSV files not found."; exit 1; fi
if [ -z "$categorylinks" ]; then echo "CategoryLinks CSV files not found."; exit 1; fi

sudo neo4j-admin database import full \
    --delimiter="\t" \
    --report-file=/tmp/import-wiki.log \
    --id-type=INTEGER \
    --nodes=Page=page_header.csv,"/home/gyde/Documents/sparkwiki/outputs/page/filt_normal_pages/*.csv.gz" \
    --nodes=Page:Category=page_header.csv,"/home/gyde/Documents/sparkwiki/outputs/page/filt_category_pages/*.csv.gz" \
    --relationships=LINKS_TO=pagelinks_header.csv,"/home/gyde/Documents/sparkwiki/outputs/filt_pagelinks/*.csv.gz" \
    --relationships=BELONGS_TO=categorylinks_header.csv,"/home/gyde/Documents/sparkwiki/outputs/filt_categorylinks/*.csv.gz" \
    --verbose \
    --overwrite-destination

