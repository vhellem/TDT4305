"""
Try to run the file with command: spark-submit alternative_listings.py 11398231 2016-12-15 10 10 20.
For more info, visit: http://spark.apache.org/docs/latest/quick-start.html#self-contained-applications.
"""

from pyspark import SparkContext
import sys
from math import sqrt, radians, sin, cos, asin
from datetime import datetime, timedelta

sc = SparkContext("local", "TF-IDF")
sc.setLogLevel("ERROR")

print("Alternative_listings Assignment")

print("Passed arguments " + str(sys.argv))

arguments = sys.argv[1:]

listing_id = str(arguments[0])
date = arguments[1]
x_percent_price = arguments[2]
y_kilometer = float(arguments[3])
n = int(arguments[4])


def haversine(lat1, lon1, lat2, lon2):
# convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def remove_dollar(line, indeks):
    line[indeks] = float(line[indeks].replace('$', '').replace(',', ''))
    return line

def toTSVLine(data):
    return '\t'.join(str(d) for d in data)

#Loads in files and prepares them

listings_m = sc.textFile("airbnb_datasets/listings_us.csv", use_unicode=False).map(lambda x: x.split('\t'))
calendar_m = sc.textFile("airbnb_datasets/calendar_us.csv", use_unicode=False).map(lambda x: x.split('\t'))

header_listing =listings_m.first()
header_calendar = calendar_m.first()

index_price = header_listing.index('price')
dataset_listing = listings_m.filter(lambda x: x != header_listing)
dataset_listing = dataset_listing.map(lambda x: remove_dollar(x, index_price)).cache()
dataset_calendar = calendar_m.filter(lambda x: x != header_calendar).cache()

#indexes



index_city = header_listing.index("city")
index_room_type = header_listing.index('room_type')
index_reviews_per_month = header_listing.index('reviews_per_month')
index_host_id = header_listing.index('host_id')
index_listing_id = header_listing.index('id')
index_longitude = header_listing.index("longitude")
index_latitude = header_listing.index("latitude")
index_rating = header_listing.index('review_scores_rating')
index_url = header_listing.index('listing_url')

index_listing_name = header_listing.index('name')
index_amenities = header_listing.index('amenities')

index_available = header_calendar.index('available')
index_cal_listing_id = header_calendar.index('listing_id')
index_cal_date = header_calendar.index('date')

actual_listing = dataset_listing.filter(lambda x: str(x[index_listing_id])==str(listing_id)).cache()

room_type = actual_listing.map(lambda x: x[index_room_type]).take(1)[0]
latitude = actual_listing.map(lambda x: x[index_latitude]).take(1)[0]
longitude = actual_listing.map(lambda x: x[index_longitude]).take(1)[0]
price = actual_listing.map(lambda x: x[index_price]).take(1)[0]
city = actual_listing.map(lambda x: x[index_city]).take(1)[0]
name = actual_listing.map(lambda x: x[index_listing_name]).take(1)[0]
amenities = actual_listing.map(lambda x:  "Amenities: " + str(len(x[index_amenities].split(",")))).take(1)[0]
url = actual_listing.map(lambda x:  x[index_url]).take(1)[0]
rating = actual_listing.map(lambda x:  x[index_rating]).take(1)[0]



availableListingsDate = (dataset_calendar.filter(lambda x: x[index_cal_date]==date)
                     .filter(lambda x: x[index_available]=="t"))\
                     .map(lambda x: (x[index_cal_listing_id],x[index_cal_date] )) #needs date to be able to join?




availableListingsMatchingCritera = (dataset_listing.filter(lambda x: x[index_price]<price*(1+float(x_percent_price)/100)) #not more expensive
                                    .filter(lambda x: x[index_room_type]==room_type) #same room type
                                    .filter(lambda x: haversine(float(latitude), float(longitude), float(x[index_latitude]), float(x[index_longitude]))< y_kilometer)#within distance - should be first?
                                    .map(lambda x: (x[index_listing_id], (x[index_listing_name], "Amenities: " + str(len(x[index_amenities].split(","))), x[index_price], haversine(float(latitude), float(longitude), float(x[index_latitude]), float(x[index_longitude]))))))


availableListings = availableListingsMatchingCritera.join(availableListingsDate)


availableListings = availableListings.map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3]))

availableListingsTop = sc.parallelize(availableListings.takeOrdered(n, key = lambda x: x[-1])).map(lambda x: toTSVLine(x))

availableListingsTop.coalesce(1).saveAsTextFile("alternative_listings.tsv")



sc.stop()
