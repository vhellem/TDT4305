from pyspark import SparkContext
import os
os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
sc = SparkContext("local", "Assignment 1")
sc.setLogLevel("ERROR")
from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(sc)
from pyspark.sql.types import DoubleType
tweets = sc.textFile("data/geotweets.tsv").map(lambda x: x.split('\t'))
columns = ['utc_time', 'country_name', "country_code", "place_type", "place_name", "language", "username", "user_screen_name", "timezone_offset", "number_of_friends", "tweet_text", "latitude", "longitude"]


df = tweets.toDF(columns)

print(df.count())

print(df.select("username").distinct().count())

print(df.select("country_name").distinct().count())

print(df.select("place_name").distinct().count())

print(df.select("language").distinct().count())

changedType = df.withColumn("latitude", df["latitude"].cast("double")).withColumn("longitude", df["longitude"].cast("double"))

print(changedType.agg({"latitude": "min"}).collect()[0])
print(changedType.agg({"latitude": "max"}).collect()[0])
print(changedType.agg({"longitude": "min"}).collect()[0])
print(changedType.agg({"longitude": "max"}).collect()[0])




