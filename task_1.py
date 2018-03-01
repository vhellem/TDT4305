from pyspark import SparkContext
import os

os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
sc = SparkContext("local", "Assignment 1")
sc.setLogLevel("ERROR")

tweets = sc.textFile("data/geotweets.tsv").map(lambda x: x.split('\t')).sample(False, 0.1, 5)


columns = ['utc_time', 'country_name', "country_code", "place_type", "place_name", "language", "username", "user_screen_name", "timezone_offset", "number_of_friends", "tweet_text", "latitude", "longitude"]

file = open("result_1.tsv", "w")

#Task 1.a
file.write(str(tweets.count()) + "\n")




#Task 1.b
usernames = tweets.map(lambda x: x[columns.index("username")]).distinct().count()
file.write(str(usernames) + "\n")

#Task 1.c
countries = tweets.map(lambda x: x[columns.index("country_name")]).distinct().count()
file.write(str(countries) + "\n")

#Task 1.d
places = tweets.map(lambda x: x[columns.index("place_name")]).distinct().count()
file.write(str(places) + "\n")

#Task 1.e
languages = tweets.map(lambda x: x[columns.index("language")]).distinct().count()
file.write(str(languages) + "\n")

#Task 1.f
lat = tweets.map(lambda x: float(x[columns.index("latitude")])).min().count()
file.write(str(lat) + "\n")

#Task 1.g
long = tweets.map(lambda x: float(x[columns.index("longitude")])).min().count()
file.write(str(long)+ "\n")

#Task 1.h
lat = tweets.map(lambda x: float(x[columns.index("latitude")])).max()
file.write(str(lat) + "\n")

#Task 1.i
long = tweets.map(lambda x: float(x[columns.index("longitude")])).max()
file.write(str(long) + "\n")

#Task 1.j
avgLength = tweets.map(lambda x: len(x[columns.index("tweet_text")])).stats().mean()
file.write(str(avgLength) + "\n")

#Task 1.f
avgLength = tweets.map(lambda x: len(x[columns.index("tweet_text")].split(" "))).stats().mean()
file.write(str(avgLength) + "\n")



file.close()