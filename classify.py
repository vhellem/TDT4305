from pyspark import SparkContext
import os
import numpy as np
os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
sc = SparkContext("local", "Assignment 1")
sc.setLogLevel("ERROR")

import argparse

columns = ['utc_time', 'country_name', "country_code", "place_type", "place_name", "language", "username", "user_screen_name", "timezone_offset", "number_of_friends", "tweet_text", "latitude", "longitude"]
parser = argparse.ArgumentParser()

parser.add_argument('-training', '-t', help='Path to training set', type=str)
parser.add_argument('-input', '-i', help='Path to input file', type=str)
parser.add_argument('-output', '-o', help='Path to output file', type=str)
args = parser.parse_args()



def bitCount(tweet, inputWords):
    return np.array([1 if word in tweet else 0 for word in inputWords])


def bayesClassifier(countVector, countPlace, totalTweets):
    start = countPlace/totalTweets
    product = np.prod(np.divide(countVector, countPlace))
    return start*product


def run(inputFile, trainingPath, outputPath):
    inputWords = open(inputFile).readline().lower().split(" ")
    raw = sc.textFile(trainingPath).map(lambda x: x.split('\t')).sample(False, 0.1, 5)
    tweets = raw.map(lambda x: (x[columns.index('place_name')], x[columns.index('tweet_text')].lower().split(' ')))

    #Creates a row with (place name, (tweet text))
    numOfTweets = tweets.count()

    tweets = (tweets.map(lambda x: (x[0],  (bitCount(x[1], inputWords), 1)))
    #Bit vector with (place name, (bit vector, counter))
              .reduceByKey(lambda x, y: (np.add(x[0], y[0]), x[1] + y[1]))
              # Reduces all tweets to (place name, (count of words, count of tweets))
              .map(lambda x: (x[0], bayesClassifier(x[1][0], x[1][1], numOfTweets))))
                #Calculates probability for each place of the given tweet being from that place



    output = open(outputPath, "w")
    maxPlace = (tweets.max(key=lambda x: x[1]))
    maxProb = maxPlace[1]
    if(maxProb==0):
        return
    #Filter away all elements that does not have the max probability
    places = tweets.filter(lambda x: x[1]==maxProb).collect()

    for place, _ in places:
        output.write(place+"\t")
    output.write(str(maxProb))






run(args.input, args.training, args.output)


