#!/bin/sh

# Delimiter and data directory
delim="\t"
data_dir="/home/gyde/Documents/sparkwiki/outputs"

# Paths to filtered data files
normal_pages=$(ls $data_dir/page/filt_normal_pages/*.csv.gz | tr '\n' ',')
category_pages=$(ls $data_dir/page/filt_category_pages/*.csv.gz | tr '\n' ',')
pagelinks=$(ls $data_dir/filt_pagelinks/*.csv.gz | tr '\n' ',')
categorylinks=$(ls $data_dir/filt_categorylinks/*.csv.gz | tr '\n' ',')

# Remove trailing commas from file lists
normal_pages=${normal_pages%,}
category_pages=${category_pages%,}
pagelinks=${pagelinks%,}
categorylinks=${categorylinks%,}

# Header files
normal_pages_header="page_header.csv"
category_pages_header="page_header.csv"
pagelinks_header="pagelinks_header.csv"
categorylinks_header="categorylinks_header.csv"

# Run the Neo4j import command
neo4j-admin database import full neo4j \
    --delimiter="${delim}" \
    --report-file=/tmp/import-wiki.log \
    --id-type=INTEGER \
    --nodes=Page="${normal_pages_header},${normal_pages}" \
    --nodes=Page:Category="${category_pages_header},${category_pages}" \
    --relationships=LINKS_TO="${pagelinks_header},${pagelinks}" \
    --relationships=BELONGS_TO="${categorylinks_header},${categorylinks}" \
    --skip-bad-relationships=true \
    --bad-tolerance=50000 \
    --verbose \
    --overwrite-destination=true

