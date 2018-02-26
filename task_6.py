from pyspark import SparkContext
import os
import datetime
#os.environ["PYSPARK_PYTHON"]="python3"
#os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
sc = SparkContext("local", "Assignment 1")
sc.setLogLevel("ERROR")

tweets = sc.textFile("data/geotweets.tsv").map(lambda x: x.split('\t')).sample(False, 0.1, 5)
