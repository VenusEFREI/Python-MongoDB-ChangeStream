import os
import pymongo
from pymongo import MongoClient
from datetime import datetime 

# Connexion au deamon
app = MongoClient('localhost', 27017)

# Base 1 qui recupère toutes les données sans sélection
db1 = app["database_1"]
collection1 = db1["events"]
# Base 2 qui filtre
db2 = app["database_2"]
collection2 = db2["processed"]

#### On écoute la collection et on est notifié après chaque insertion dans la collection
pipeline = [{'$match': {'operationType': 'insert'}}]
with collection1.watch(pipeline) as stream:
    for insert_change in stream:
        result = insert_change["fullDocument"]
        data = result["Pays"]
        tag = collection2.find_one({"Pays": data})
        if tag == None:  # si aucune corrspondance dans la base2 (propre) => on insert
            collection2.insert_one(result)
            print("nouvelle insertion")
        else:   # Au cas contraire on met juste à jour la date de l'existant
            collection2.update_one({ "Pays": data}, { "$set": {"Date": datetime.now()}})
            print("Date mise à jour")