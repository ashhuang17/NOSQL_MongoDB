#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING
import pprint


# In[2]:


#establishing connection
host = "ec2-54-67-89-95.us-west-1.compute.amazonaws.com"
client = MongoClient(host, 9210)

#accessing databases
db = client.nosql
db_names = db.list_collection_names()
print("Collection Names:")
pprint.pprint(db_names)
collection = db["aq"]


# In[3]:


#Get all keys
mydoc = collection.find({})

for x in mydoc:
    l=list(x.keys())
    print(l)
    break


# In[4]:


## Question 1 - How many total records are there in this air quality collection ? ##

count = collection.count_documents({})

print("Answer: The total records in this air quality collection is", count)


# In[21]:


## Question 2 - What is the data type of the columns StateName, ReportYear, and Value ? ##

q2 = collection.find_one({})

print("Answer:")

print("Type of StateName:", type(q2["StateName"]))
print("Type of ReportYear:", type(q2["ReportYear"]))
print("Type of Value:", type(q2["Value"]))


# In[19]:


## Question 3 - What was the air quality metric value for MeasureId 83 in the Santa Barbara county in the state of California in 2013 ? ##

q3 = collection.find_one({"MeasureId":83,
                         "$and": [{"CountyName":'Santa Barbara'},
                                  {"StateName":'California'},
                                  {"ReportYear": 2013}]})
value = q3["Value"]

print("Answer:")
print("The air quality metric value is", value)


# In[7]:


## Question 4 - What is the MeasureName of MeasureId 85 ?##

q4 = collection.find_one({"MeasureId":85})

name = q4["MeasureName"]

print("Answer:", name)


# In[14]:


## Question 5 - Which state and county have the highest recorded air quality value of MeasureId 85 in any year ? ##

q5 = collection.find({"MeasureId":85}).sort("Value", DESCENDING)

state = q5[0]["StateName"]
county = q5[0]["CountyName"]

print("Answer:")
print("The State with the highest air quality value:", state)
print("The County with the highest air quality value:", county)


# In[16]:


## Question 6 - Print the average of air quality value and the unit for MeasureId 87, State - Florida, CountyName - Pinellas, for years between 2002 - 2008 ? ##

q6 = collection.aggregate([
    {"$match": {"MeasureId":87,
                "$and":[{"StateName":"Florida"},
                        {"CountyName":"Pinellas"},
                        {"ReportYear":{"$gte":2002}},
                        {"ReportYear":{"$lte":2008}}]}},
    {"$group": {"_id":"$Unit","average":{"$avg":"$Value"}}}
]);

for i in q6:
    ave = i["average"]
    unit = i["_id"]
print("Answer:")
print("Average Air Quality Value(Unit):",ave,"/", unit)


# In[36]:


## Question 7 - Based on the data provided in the air quality collection, which state do you believe has better air quality- California or Texas ? Why? Call out any assumptions you make. ##


texas = collection.aggregate([
    {"$match": {"StateName":"Texas",
                "$and":[{"Unit":"µg/m³"},
                        {"ReportYear":{"$gte":1999}}]}},
    {"$group": {"_id":"$Unit","average":{"$avg":"$Value"}}}
]);

for i in texas:
    ave = i["average"]
    unit = i["_id"]
    print("Answer:")
    print("Average of air quality from 1999 to the last report year in Texas:")
    print(ave, unit)
    print("  ")


cali = collection.aggregate([
    {"$match": {"StateName":"California",
                "$and":[{"Unit":"µg/m³"},
                        {"ReportYear":{"$gte":1999}}]}},
    {"$group": {"_id":"$Unit","average":{"$avg":"$Value"}}}
]);

for i in cali:
    ave = i["average"]
    unit = i["_id"]
    print("Average of air quality from 1999 to the last report year in California:")
    print(ave, unit)
    print("  ")
    
print("As seen, we can see that the air quality based on the provided data, it indicates that Texas has a better air quality overall. It has shown that Texas has approximately 9 µg/m³ and California has approximately 12 µg/m³ in average from year 1999 to the last reported year. In conclusion, Texas has better air quality compare to California.")


# In[ ]:





# In[ ]:




