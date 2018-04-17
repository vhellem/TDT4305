from pyspark import SparkContext
import os
import numpy as np
os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"
sc = SparkContext("local", "Assignment 1")
sc.setLogLevel("ERROR")
import itertools



columns = ['utc_time', 'country_name', "country_code", "place_type", "place_name", "language", "username", "user_screen_name", "timezone_offset", "number_of_friends", "tweet_text", "latitude", "longitude"]



def trainOnDataset(filename, wordList):
    #Maps place names to the tweet text, tweet text is separared by spaces and converted into a set of words
    raw = (sc.textFile(filename).map(lambda x: x.split('\t')))
    tweets = raw.map(lambda x: (x[columns.index('place_name')], set(x[columns.index('tweet_text')].lower().split(' '))))
    numOfTweets = tweets.count()
    Tplace = (raw.map(lambda x: (x[columns.index('place_name')], 1))
              .reduceByKey(lambda x, y: x+y))

    TplaceWord = (tweets.flatMap(lambda x: combineWordWithPlace(x[0], x[1])).filter(lambda x: x[0][1] in wordList)
                  .reduceByKey(lambda x, y: x+y)
                    .map(lambda x: (x[0][1], (x[0][0], x[1])))
                    )


    return TplaceWord, Tplace, numOfTweets

def combineWordWithPlace(place, wordList):
    #Returns the place combined with all the word, along with a counter
    result = []
    for word in wordList:
        result.append(((place, word), 1))
    return result


def run(inputFile, trainingPath, outputPath):
    inputWords = inputFile.split(" ")
    TplaceWord, Tplace, count = trainOnDataset(trainingPath, inputWords)
    probs = Tplace.map(lambda x: (x[0], x[1]/count))

    #TplaceWord = TplaceWord.filter(lambda x: x[0] in inputWords).cache()
    """
    #Do it all in one pass
    TplaceWord = TplaceWord.map(lambda x: x[1])
    numOfWords = len(inputWords)
    TplaceWord = TplaceWord.reduceByKey(lambda x, y: x*y)
    probs = probs.join(TplaceWord).join(Tplace)
    probs = probs.map(lambda x: (x[0], x[1][0][0]*x[1][0][1]/(x[1][1]*numOfWords)))
    """
    #Do it for each word
    for word in inputWords:
        wordCountInPlaces = TplaceWord.filter(lambda x: x[0]==word).map(lambda x: x[1])

        probs = probs.join(wordCountInPlaces).join(Tplace)
        #This will look like (place, ((Probability, Number of Times this word is mentioned from that place), Number of Tweets from that place)
        probs = probs.map(lambda x: (x[0], x[1][0][0]*x[1][0][1]/x[1][1]))

    out = (probs.top(1, key=lambda x: x[1]))
    output = open(outputPath, "w")
    print(out)
    
    output.write("\t".join(out))






run("great job", "data/geotweets.tsv", "lol")


