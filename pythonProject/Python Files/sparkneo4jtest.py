from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TestNeo4jConnection") \
    .config("spark.neo4j.url", "bolt://localhost:7687") \
    .config("spark.neo4j.user", "neo4j") \
    .config("spark.neo4j.password", "shurtagal") \
    .getOrCreate()

# Define the Cypher query
query = "MATCH (n) RETURN n LIMIT 10"

# Load data from Neo4j
df = spark.read.format("org.neo4j.spark.DataSource") \
    .option("query", query) \
    .load()

# Show the results
df.show()
