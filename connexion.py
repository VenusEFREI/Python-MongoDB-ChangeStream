import os
import pymongo
from pymongo import MongoClient

app = MongoClient('localhost', 27017)
db1 = app["database_1"]
collection1 = db1["events"]

#### On écoute la collection et on est notifié après chaque insertion dans la collection
pipeline = [{'$match': {'operationType': 'insert'}}]
with collection1.watch(pipeline) as stream:
    for insert_change in stream:
        print(insert_change)

