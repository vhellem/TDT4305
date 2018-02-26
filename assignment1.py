from pyspark import SparkContext
import os

os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
sc = SparkContext("local", "Assignment 1")
sc.setLogLevel("ERROR")

tweets = sc.textFile("data/geotweets.tsv").map(lambda x: x.split('\t')).sample(False, 0.1, 5)


columns = ['utc_time', 'country_name', "country_code", "place_type", "place_name", "language", "username", "user_screen_name", "timezone_offset", "number_of_friends", "tweet_text", "latitude", "longitude"]


#Task 1.a
#print(tweets.count())

#Task 1.b
usernames = tweets.map(lambda x: x[columns.index("username")]).distinct()
#print(usernames.count())

#Task 1.c
countries = tweets.map(lambda x: x[columns.index("country_name")]).distinct()
#print(countries.count())

#Task 1.d
places = tweets.map(lambda x: x[columns.index("place_name")]).distinct()
#print(places.count())

#Task 1.e
languages = tweets.map(lambda x: x[columns.index("language")]).distinct()
print(languages.count())

#Task 1.f


