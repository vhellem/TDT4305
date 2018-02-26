from pyspark import SparkContext
import os
import datetime
os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
sc = SparkContext("local", "Assignment 1")
sc.setLogLevel("ERROR")

tweets = sc.textFile("data/geotweets.tsv").map(lambda x: x.split('\t')).sample(False, 0.1, 5)


columns = ['utc_time', 'country_name', "country_code", "place_type", "place_name", "language", "username", "user_screen_name", "timezone_offset", "number_of_friends", "tweet_text", "latitude", "longitude"]

def getHourFromTimeStamp(utc_time, offset):
    localTimeInSeconds=utc_time/1000+offset
    actualTime = datetime.datetime.fromtimestamp(localTimeInSeconds)
    return actualTime.hour

def getMaxTweets(x, y):
    if x[1]>y[1]:
        return x
    return y

mostTraffickingHour = (tweets.map(lambda x: ((x[columns.index("country_name")],
                                             getHourFromTimeStamp(int(x[columns.index("utc_time")]), int(x[columns.index("timezone_offset")]))), 1))
                                            .reduceByKey(lambda x, y: x+y)
                                            .map(lambda x: (x[0][0], (x[0][1], x[1])))
                                            .reduceByKey(lambda x, y: getMaxTweets(x, y)))

print(mostTraffickingHour)
print(mostTraffickingHour.take(2))
