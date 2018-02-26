from pyspark import SparkContext
import os
os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
sc = SparkContext("local", "Assignment 1")
sc.setLogLevel("ERROR")

tweets = sc.textFile("data/geotweets.tsv").map(lambda x: x.split('\t')).sample(False, 0.1, 5)

stopwords = sc.textFile("data/stop_words.txt")
columns = ['utc_time', 'country_name', "country_code", "place_type", "place_name", "language", "username", "user_screen_name", "timezone_offset", "number_of_friends", "tweet_text", "latitude", "longitude"]

top5Cities = (tweets.filter(lambda x: x[columns.index("country_code")]== "US" and x[columns.index("place_type")]== "city")
          .map(lambda x: (x[columns.index("place_name")], 1)).reduceByKey(lambda x, y: x+y).takeOrdered(5, lambda x: (-x[1], x[0]))
          )

print(top5Cities)
top5 = sc.parallelize(top5Cities)
for city, _ in top5Cities:
    frequentWords = (tweets.filter(lambda x: x[columns.index("place_name")] == city)
                     .flatMap(lambda x: x[columns.index("tweet_text")].split(" "))
                     .filter(lambda x: len(x) > 2)
                     .map(lambda x: x.lower())
                     .subtract(stopwords)
                     .map(lambda x: (x, 1))
                     .reduceByKey(lambda x, y: x + y)
                     .takeOrdered(10, lambda x: (-x[1], x[0])))
    print(city, frequentWords)
    top5.join(frequentWords)

print(top5)

