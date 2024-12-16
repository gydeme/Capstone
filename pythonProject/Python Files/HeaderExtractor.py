import bz2
import re
import os
import csv
import random

def extract_table_schema(file_path, encoding='latin-1'):
    """
    Extract the table schema (column names) from a CREATE TABLE statement.
    """
    schema_lines = []
    schema_extracted = False
    columns = []

    with bz2.open(file_path, 'rt', encoding=encoding, errors='replace') as file:
        for line in file:
            line = line.strip()

            if line.startswith("CREATE TABLE"):
                schema_extracted = True
            elif schema_extracted:
                schema_lines.append(line)
                # Look for lines defining columns, which typically look like `column_name type`
                column_match = re.match(r'`(\w+)`', line)
                if column_match:
                    columns.append(column_match.group(1))  # Extract the column name
                if line.endswith(');'):
                    break

    return columns

def extract_sample_data(file_path, num_samples=10, encoding='latin-1'):
    """
    Extract a random sample of `num_samples` rows from the INSERT INTO values.
    """
    sample_data = []

    with bz2.open(file_path, 'rt', encoding=encoding, errors='replace') as file:
        for line in file:
            if "INSERT INTO" in line:
                # Find the rows between the parentheses
                values = re.findall(r'\(([^)]*)\)', line)
                if values:
                    # Randomly select `num_samples` rows from the available values
                    sample_data = random.sample(values, min(num_samples, len(values)))
                    break

    return sample_data

def save_samples_to_csv(file_path, dump_name, sample_data, columns):
    """
    Save the sample data to a CSV file with the table schema as the header in /home/gyde/Documents/bzsets/csv.
    """
    output_dir = '/home/gyde/Documents/bzsets/csv'
    os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists

    # Define the output CSV file name
    output_file = os.path.join(output_dir, f"{dump_name.replace(' ', '_')}_sample.csv")

    # Write sample data to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # Write the table schema as the header (column names)
        writer.writerow(columns)

        for row in sample_data:
            # Split the values in each row by commas and write to CSV
            parsed_row = row.split(',')
            writer.writerow(parsed_row)

    print(f"Sample data saved to {output_file}")

def print_schema_and_samples(file_path, dump_name, num_samples=10):
    """
    Print the schema and save sample data for the specified dump, using the schema as the CSV header.
    """
    columns = extract_table_schema(file_path)
    sample_data = extract_sample_data(file_path, num_samples=num_samples)

    print(f"{dump_name} Schema and Sample Data:")
    print("Schema (Columns):")
    print(columns)

    print("\nSample Data:")
    for sample in sample_data:
        print(sample)

    # Save the sample data to a CSV file, using the schema (columns) as the header
    save_samples_to_csv(file_path, dump_name, sample_data, columns)

# Paths to your dumps
dumps = {
    # "2018 Pagelinks": '/home/gyde/Documents/bzsets/20180920/enwiki-20180920-pagelinks.sql.bz2',
    # "2018 Categorylinks": '/home/gyde/Documents/bzsets/20180920/enwiki-20180920-categorylinks.sql.bz2',
    # "2018 Page": '/home/gyde/Documents/bzsets/20180920/enwiki-20180920-page.sql.bz2',
    # "2018 Redirect": '/home/gyde/Documents/bzsets/20180920/enwiki-20180920-redirect.sql.bz2',
    # "2018 Category": '/home/gyde/Documents/bzsets/20180920/enwiki-20180920-category.sql.bz2',
    "2024 Pagelinks": '/home/gyde/Documents/bzsets/20240620/enwiki-20240620-pagelinks.sql.bz2',
    "2024 Categorylinks": '/home/gyde/Documents/bzsets/20240620/enwiki-20240620-categorylinks.sql.bz2',
    "2024 Page": '/home/gyde/Documents/bzsets/20240620/enwiki-20240620-page.sql.bz2',
    "2024 Redirect": '/home/gyde/Documents/bzsets/20240620/enwiki-20240620-redirect.sql.bz2',
    "2024 Category": '/home/gyde/Documents/bzsets/20240620/enwiki-20240620-category.sql.bz2',
    "2024 LinkTarget": '/home/gyde/Documents/bzsets/20240620/enwiki-20240620-linktarget.sql.bz2'
}

# Process schemas and sample data for all dumps
for dump_name, file_path in dumps.items():
    print_schema_and_samples(file_path, dump_name, num_samples=10)
    print("\n" + "="*80 + "\n")
