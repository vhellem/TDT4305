"""
 spark-submit tf_idf.py /Users/Vegard/DataAnalysisWithSpark2/airbnb_datasets -n "East Village"

"""

import sys
import re
from pyspark import SparkContext
arguments = sys.argv[1:]
from math import log

sc = SparkContext("local", "TF-IDF")
sc.setLogLevel("ERROR")

print("TF_IDF Assignment")

path_to_dataset = arguments[0]
flag = arguments[1]
search_id = arguments[2]




#read in Data
listings_m = sc.textFile(path_to_dataset +"/listings_us.csv", use_unicode=False).map(lambda x: x.split('\t'))
neighbourhood_m = sc.textFile(path_to_dataset +"/listings_ids_with_neighborhoods.tsv", use_unicode=False).map(lambda x: x.split('\t'))
#headers and data without header
header_listing = listings_m.first()
header_neighbourhood = neighbourhood_m.first()
dataset_listing = listings_m.filter(lambda x: x != header_listing)
dataset_neighborhood = neighbourhood_m.filter(lambda x: x!= header_neighbourhood)

#indexes used
index_listing_id = header_listing.index('id')
index_neighborhood = header_listing.index('neighborhood_overview')
index_description = header_listing.index('description')

index_neighborhood_id = header_neighbourhood.index('id')
index_neighborhood = header_neighbourhood.index('neighbourhood')


#Computes idf
#filters away no descriptions
validData = dataset_listing.filter(lambda x: x[index_description] != "")
number_of_documents = validData.count() #counts number of documents
regex = re.compile('[^a-zA-Z ]')
descriptions = (validData.map(lambda x: x[index_description])
         .map(lambda line: set((regex.sub('', line.lower()).split(" "))))) #cleans Data, removes duplicates


idf = (descriptions.flatMap(lambda x: x) #flats out words
                        .map(lambda x: (x, 1)) #add counter
                        .reduceByKey(lambda x, y: x+y) #how many documents contain the word
                        .map(lambda x: (x[0], log(float(number_of_documents)/float(x[1]), 10)))) #idf




def toTSVLine(data):
    return '\t'.join(str(d) for d in data)


if flag =="-1":
    listings = dataset_listing.filter(lambda x: x[index_listing_id]==search_id).map(lambda x: x[index_description])

else:
    #For simplicity, a neighbourhood is seen as one document
    listings = dataset_neighborhood.filter(lambda x: x[index_neighborhood] == search_id)

    dataset_listing = dataset_listing.map(lambda x: (x[index_listing_id], x[index_description]))

    listings = listings.join(dataset_listing).map(lambda x:  x[1][1])



regex = re.compile('[^a-zA-Z ]')
cleanedData = (listings.flatMap(lambda line: set(line.lower().replace("[\\d[^\\w\\s]]+", " ")
                                                 .replace("(\\s{2,})", " ").split(" ")))) #Gets all words in the document(s)

total_terms = cleanedData.distinct().count() #Total terms in the collection

tf = (cleanedData.map(lambda x: (x, 1)) #add counter
      .reduceByKey(lambda x, y: x+y) #add total count of terms
      .map(lambda x: (x[0], float(x[1])/float(total_terms)))) #tf



top100 = (tf.join(idf)
        .map(lambda x: (x[0], x[1][0]*x[1][1]))
          .top(100, key = lambda x: x[1]))

print(top100)


top100 = top100.map(lambda x: toTSVLine(x))
top100.coalesce(1).saveAsTextFile("tf_idf_results.tsv")

#Returns 100 words with highest term frequency




