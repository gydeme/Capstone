#!/bin/sh
delim="\t"
data_dir="/home/gyde/Documents/sparkwiki/outputs"

# Collect the actual file paths for each type of data
normal_pages=$(ls $data_dir/page/normal_pages/part-*.csv | tr '\n' ',')
category_pages=$(ls $data_dir/page/category_pages/part-*.csv | tr '\n' ',')
pagelinks=$(ls $data_dir/pagelinks/part-*.csv | tr '\n' ',')
categorylinks=$(ls $data_dir/categorylinks/part-*.csv | tr '\n' ',')

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

# Neo4j import command using actual file paths
neo4j-admin database import full \
    --delimiter=$delim \
    --report-file=/tmp/import-wiki.log \
    --id-type=INTEGER \
    --nodes=Page=page_header.csv,"$normal_pages" \
    --nodes=Page:Category=page_header.csv,"$category_pages" \
    --relationships=LINKS_TO=pagelinks_header.csv,"$pagelinks" \
    --relationships=BELONGS_TO=categorylinks_header.csv,"$categorylinks"

