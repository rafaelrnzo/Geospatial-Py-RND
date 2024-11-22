from pyspark.sql import SparkSession

# MongoDB connection details
mongo_uri = "mongodb+srv://devadmin:2553669874123654@dev218.kke0v.mongodb.net/"
database_name = "climate_data"
collection_name = "temperature_data"

# Initialize Spark session with MongoDB Connector
spark = SparkSession.builder \
    .appName("MongoDBIntegration") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .config("spark.mongodb.input.uri", mongo_uri + database_name + "." + collection_name) \
    .config("spark.mongodb.output.uri", mongo_uri + database_name + "." + collection_name) \
    .getOrCreate()

# Load data from MongoDB into Spark DataFrame
spark_df = spark.read.format("mongo").load()
