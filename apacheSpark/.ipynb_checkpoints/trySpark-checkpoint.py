from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd

def get_data_from_mongo(mongo_uri, db_name, collection_name):
    try:
        spark = SparkSession.builder \
            .appName("MongoDBIntegration") \
            .config("spark.mongodb.read.connection.uri", mongo_uri) \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.0.0") \
            .getOrCreate()

        # Load data from MongoDB into a Spark DataFrame
        df = spark.read.format("mongodb").option("database", db_name).option("collection", collection_name).load()

        print("Successfully fetched data from MongoDB.")
        
        # Convert to Pandas DataFrame
        pandas_df = df.select("_id", "time", "latitude", "longitude", "temperature").toPandas()
        return pandas_df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# MongoDB connection details
mongo_uri = (
    "mongodb+srv://devadmin:2553669874123654@dev218.kke0v.mongodb.net/"
    "climate_data?retryWrites=true&w=majority&appName=dev218"
)
database_name = "climate_data"
collection_name = "temperature_data"

# Fetch and process data
data = get_data_from_mongo(mongo_uri, database_name, collection_name)

if data is not None:
    plt.scatter(data["latitude"], data["temperature"], alpha=0.5, c="blue")
    plt.title("Temperature by Latitude")
    plt.xlabel("Latitude")
    plt.ylabel("Temperature (°C)")
    plt.grid(True)
    plt.show()
