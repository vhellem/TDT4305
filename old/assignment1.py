#spark-submit assignment1.py
#importing help functions
from help_functions import remove_empty_lines, remove_dollar, toCSVLine

from pyspark import SparkContext


sc = SparkContext("local", "Assignment 1")
sc.setLogLevel("ERROR")

listings_m = sc.textFile("airbnb_datasets/listings_us.csv", use_unicode=False).map(lambda x: x.split('\t'))
calendar_m = sc.textFile("airbnb_datasets/calendar_us.csv", use_unicode=False).map(lambda x: x.split('\t'))
reviews_m = sc.textFile("airbnb_datasets/reviews_us.csv", use_unicode=False).map(lambda x: x.split('\t'))

resultFile = open('results_assignment1.txt', 'w')

# headers
header_listing =listings_m.first()
header_calendar = calendar_m.first()
header_reviews = reviews_m.first()

# Datasets without headers
dataset_listing = listings_m.filter(lambda x: x != header_listing).cache()
dataset_calendar = calendar_m.filter(lambda x: x != header_calendar).cache()
dataset_reviews = reviews_m.filter(lambda x: x != header_reviews).cache()


#Setup for task
#Indexes to be used in later exercises

index_price = header_listing.index('price')
index_city = header_listing.index("city")
index_room_type = header_listing.index('room_type')
index_reviews_per_month = header_listing.index('reviews_per_month')
index_host_id = header_listing.index('host_id')
index_listing_id = header_listing.index('id')
index_longitude = header_listing.index("longitude")
index_latitude = header_listing.index("latitude")
index_amenities = header_listing.index("amenities")

index_available = header_calendar.index('available')
index_cal_listing_id = header_calendar.index('listing_id')

index_review_listing_id = header_reviews.index("listing_id")
index_reviewer_id = header_reviews.index("reviewer_id")
index_reviewer_name =header_reviews.index("reviewer_name")


#Cleaning data


#Remove $ and , signs in number, and empty lines in reviews
dataset_listing = dataset_listing.map(lambda x: remove_dollar(x, index_price))
dataset_listing = dataset_listing.map(lambda x: remove_empty_lines(x, index_reviews_per_month))

#Task 2a)
#Listing ID is present in both listings, reviews, and calendar. This is the attribute linking these three datasets together

#2b) Number of distinct values for each field in the listings dataset

list = [] # Made a list for better printing of the result
for i in range(0, len(header_listing)):
    list.append(header_listing[i] + ": " + str(dataset_listing.map(lambda x: x[i]).distinct().count()))

resultFile.write('Unique elements in listings ' + str(list) + '\n')



#Task 2c)
#Find out how many cities there are and which
cities = dataset_listing.map(lambda x: x[index_city]).distinct().collect()


resultFile.write('Cities in listing dataset: ' + str(cities) + '\n')



#Task 2d)



#Task 3a)

total_price =( dataset_listing.map(lambda x: (x[index_city], (x[index_price], 1)))\
                .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])))

avg_price = total_price.map(lambda x:( x[0], x[1][0]/x[1][1]))
print(avg_price.collect())

avg_price.coalesce(1).saveAsTextFile("3a.csv")

#Task 3b)
total_price = (dataset_listing.map(lambda x: ((x[index_city], x[index_room_type]), (x[index_price], 1)))
               .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])))

avg_price = total_price.map(lambda x:( x[0], x[1][0]/x[1][1]))

print(avg_price.collect())

avg_price.coalesce(1).saveAsTextFile("3b.csv")




#Task 3c)
#Average number of reviews per month



average_reviews_per_city = (dataset_listing.map(lambda x: (x[index_city], (x[index_reviews_per_month], 1)))
                                     .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) # Total reviews per city, and total listings
                                     .map(lambda x: (x[0], x[1][0]/x[1][1]))) # Average reviews per city

average_reviews_per_city.coalesce(1).saveAsTextFile("3c.csv")


#Task 3d)

data_with_reviews= dataset_listing.filter(lambda x: x[index_reviews_per_month]!=0)

nights= 3
months = 12
percent = 0.7

total_nights_booked_per_city_per_year = (data_with_reviews.map(lambda x: (x[index_city], x[index_reviews_per_month])) # City and reviews per month in one RDD
                                         .reduceByKey(lambda x, y: (x + y)) # Total reviews per month per city (only 70% leaves reviews)
                                         .map(lambda x: (x[0], (x[1] * nights* months) / percent))) # Total nights booked per year per city

total_nights_booked_per_city_per_year.coalesce(1).saveAsTextFile("3d.csv")




#Task 3e)

estimated_amount = (total_nights_booked_per_city_per_year.join(avg_price)
                    .map(lambda x: (x[0], int([x[1][0]*x[1][1]]) )))

estimated_amount.coalesce(1).saveAsTextFile("3e.csv")



#Task 4.

#Task 4a) Compute global average number of listings per host

# Count rows in the dataset
rows = float(dataset_listing.count()) # 64649 rows

# Count number of unique hosts
unique_hosts = float(dataset_listing.map(lambda x: x[index_host_id]).distinct().count())

# Average number of listings per hosts
average_listings_per_host = rows/unique_hosts

resultFile.write("Global average number of listings per host:" + str(average_listings_per_host) + '\n')



#Task 4b) Compute percentage of hosts with more than 1 listings

hosts_with_more_than_one_listing = (dataset_listing.map(lambda x: (x[index_host_id], 1))
                    .reduceByKey(lambda x,y: x + y) # Number of listing for unique hosts
                    .filter(lambda x: x[1] > 1)#Filter out hosts with > 1 listing
                    .count()) #Count amount



percentage_of_hosts = (hosts_with_more_than_one_listing/unique_hosts)*100

resultFile.write("Percentage of hosts with more than 1 listings: " + str(percentage_of_hosts) + '\n')
# Percentage of hosts with more than 1 listings:  14.5548447679


#Task 4c) For each city, top 3 hosts with highest income.



calendarData = (dataset_calendar.filter(lambda x: x[index_available] == 'f') # Filter out those that were booked
                .map(lambda x: (x[index_cal_listing_id],1)) # Add counter
                .reduceByKey(lambda x,y: x+y)) #for Each Listing, count amount of days booked


listingData = dataset_listing.map(lambda x: (x[index_listing_id], (x[index_city], x[index_host_id], x[index_price])))

joined = listingData.join(calendarData) # ([('1591807', (('New York', '5749240', '$45.00'), 391))])


total_income_per_listing = joined.map(lambda x: ((x[1][0][1], x[1][0][0]), x[1][0][2]*x[1][1])) #Key is host_id and city, calculate price times amount of days booked

total_income_per_host = total_income_per_listing.reduceByKey(lambda x, y: x+y) #Total amount of income per host with the city


#Top 3 hosts for San Francisco
top_SF = total_income_per_host.filter(lambda x: x[0][1] == "San Francisco").top(3, key=lambda x: (x[1]))


#Top 3 hosts for New York
top_NY = total_income_per_host.filter(lambda x: x[0][1] == "New York").top(3, key = lambda x: x[1])

#Top 3 Hosts for Seattle
top_S = total_income_per_host.filter(lambda x: x[0][1] == "Seattle").top(3, key = lambda x: x[1])

resultFile.write('Top 3 hosts in San Francisco' + str(top_SF) + '\n')
resultFile.write('Top 3 hosts in New York' + str(top_NY) + '\n')
resultFile.write('Top 3 hosts in Seattle' + str(top_S) + '\n')



#Task 5a)

#top 3 guests, by number of bookings, for each city

guests = dataset_reviews.map(lambda x: (x[index_review_listing_id], (x[index_reviewer_id], x[index_reviewer_name])))
listings = dataset_listing.map(lambda x: (x[index_listing_id], x[index_city]))

joined = guests.join(listings)

total_amount_of_bookings = joined.map(lambda x: ((x[1][0][0], x[1][0][1], x[1][1]), 1)).reduceByKey(lambda x, y: x+y)

#Top 3 guests for San Francisco
top_SF = total_amount_of_bookings.filter(lambda x: x[0][2] == "San Francisco").top(3, key=lambda x: (x[1]))


#Top 3 guests for New York
top_NY = total_amount_of_bookings.filter(lambda x: x[0][2] == "New York").top(3, key = lambda x: x[1])

#Top 3 guests for Seattle
top_S = total_amount_of_bookings.filter(lambda x: x[0][2] == "Seattle").top(3, key = lambda x: x[1])

result = top_SF +top_NY+top_S
result =  sc.parallelize(result)

result.coalesce(1).saveAsTextFile("5a.csv")

#Task 5b

#Amount of times a customer has booked a listing, assuming only one night

guestAmount = dataset_reviews.map(lambda x: ((x[index_review_listing_id], x[index_reviewer_id], x[index_reviewer_name]), 1)).reduceByKey(lambda x, y: x+y)

#Rearranging key
guestAmount = guestAmount.map(lambda x: (x[0][0], (x[0][1], x[0][2], x[1])))

priceListing = dataset_listing.map(lambda x: (x[index_listing_id], x[index_price]))

joined = guestAmount.join(priceListing)

topGuest = (joined.map(lambda x: ((x[1][0][0], x[1][0][1]), x[1][0][2]*x[1][1]))
            .reduceByKey(lambda x, y: x+y).top(1, key = lambda x: x[1]))

resultFile.write("Guest with most money spent" + str(topGuest) + '\n')



#Task 6a)


import json
from shapely.geometry import shape, Point


with open('airbnb_datasets/neighbourhoods.geojson') as f:
    js = json.load(f)



long_lat_data = dataset_listing.map(lambda x:(x[index_listing_id], (x[index_longitude], x[index_latitude])))

testData = sc.textFile("airbnb_datasets/neighborhood_test.csv").map(lambda line: line.split('\t'))
neighbourhood_Data = testData.map(lambda x: (x[0], x[1]))
#Used a library to find point in polygon
def findNeighbourhood(longitude, latitude):
    longitude = float(longitude)
    latitude = float(latitude)
    point = Point(longitude, latitude)
    for feature in js['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            return feature['properties']['neighbourhood']

listingNeighbourhood = long_lat_data.map(lambda line: (line[0], findNeighbourhood(line[1][0],line[1][1])))
comparator = listingNeighbourhood.join(neighbourhood_Data)

total = comparator.count()
sameCount = comparator.filter(lambda line: line[1][0]==line[1][1]).count()

correctPercentage = float(sameCount)/float(total)*100

resultFile.write("Percentage agreed with test set: " + str(correctPercentage)+'%\n' )
percentageRDD = sc.parallelize((("Percentage agreed with test set: ", str(correctPercentage)+'%'),))

r = (percentageRDD + comparator).sortByKey(False)
r.coalesce(1).saveAsTextFile("6a.csv")



#Task 6B)

amenities = (dataset_listing.map(lambda x: (findNeighbourhood(x[index_longitude], x[index_latitude]),set(x[index_amenities].replace("{","").replace("}","").split(',')))) #maps neighbourhood to amenities
             .reduceByKey(lambda x, y: x.union(y)) #union of the sets
             .map(lambda x: (x[0], sorted(x[1]))) #sort amenities list
             .sortBy(lambda x: x[0])) #sort by neighbourhood

amenities.coalesce(1).saveAsTextFile("6b.csv")

#Task visualizing, 100 priciest listings
"""
visualizing = sc.parallelize((dataset_listing.map(lambda x: (x[index_room_type], x[index_latitude], x[index_longitude], x[index_price]))
                .sortBy(lambda x: x[3])
                .top(100, key = lambda x: x[3])))

visualizing.map(lambda x: toCSVLine(x))


visualizing.coalesce(1).saveAsTextFile("vis2100expensive.csv")
"""

resultFile.close()