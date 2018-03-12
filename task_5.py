from pyspark import SparkContext
import os
import datetime
#os.environ["PYSPARK_PYTHON"]="python3"
#os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
sc = SparkContext("local", "Assignment 1")
sc.setLogLevel("ERROR")
def toTSVLine(data):
    return '\t'.join(str(d) for d in data)

tweets = sc.textFile("data/geotweets.tsv").map(lambda x: x.split('\t'))


columns = ['utc_time', 'country_name', "country_code", "place_type", "place_name", "language", "username", "user_screen_name", "timezone_offset", "number_of_friends", "tweet_text", "latitude", "longitude"]

Cities = (tweets.filter(lambda x: x[columns.index("country_code")]== "US" and x[columns.index("place_type")]== "city")
          .map(lambda x: (x[columns.index("place_name")], 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: (-x[1], x[0]))
          )

print(Cities.take(10))
Cities.map(lambda x: toTSVLine(x)).coalesce(1).saveAsTextFile("task_5.tsv")