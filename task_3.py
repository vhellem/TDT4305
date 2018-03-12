from pyspark import SparkContext
import os

os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
sc = SparkContext("local", "Assignment 1")
sc.setLogLevel("ERROR")

def toTSVLine(data):
    return '\t'.join(str(d) for d in data)

tweets = sc.textFile("data/geotweets.tsv").map(lambda x: x.split('\t'))


columns = ['utc_time', 'country_name', "country_code", "place_type", "place_name", "language", "username", "user_screen_name", "timezone_offset", "number_of_friends", "tweet_text", "latitude", "longitude"]


countriesWithEnoughTweets = (tweets.map(lambda x: (x[columns.index("country_name")], (float(x[columns.index("latitude")]), float(x[columns.index("longitude")]), 1)))
                             .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
                             .filter(lambda x: x[1][2]>10))

averages = (countriesWithEnoughTweets.map
            (lambda x: (x[0], x[1][0]/x[1][2], x[1][1]/x[1][2])))

averages.map(lambda x: toTSVLine(x)).coalesce(1).saveAsTextFile("task_3")