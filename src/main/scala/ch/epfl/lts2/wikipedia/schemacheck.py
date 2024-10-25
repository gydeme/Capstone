from pyspark.sql import SparkSession

def print_schema(file_path, file_type):
    spark = SparkSession.builder.appName("Schema Printer").getOrCreate()
    df = spark.read.format("csv").option("header", "true").load(file_path)
    print(f"Schema for {file_type}:")
    df.printSchema()
    df.show(10)

def main():
    base_path = "/mnt/data/wikipedia/dumps/"
    prefix = "enwiki-20240620"

    files = {
        "Page": f"{base_path}{prefix}-page.sql.bz2",
        "Page Links": f"{base_path}{prefix}-pagelinks.sql.bz2",
        "Category Links": f"{base_path}{prefix}-categorylinks.sql.bz2",
        "Redirect": f"{base_path}{prefix}-redirect.sql.bz2",
        "Category": f"{base_path}{prefix}-category.sql.bz2"
    }

    for file_type, file_path in files.items():
        print_schema(file_path, file_type)

if __name__ == "__main__":
    main()

