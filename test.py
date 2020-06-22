import pymongo
from pymongo import MongoClient
import json
import os
import sys

def getConfigFromFile(jsonFilePath):
        with open(jsonFilePath) as configFile:
            config = json.load(configFile)
            return config

class IsisMongoWatcher:

    def start(self, target_1, target_2, opType):
        config = getConfigFromFile('isis-mongo-conf.json')
        param = config["DEV"]
        connexion = MongoClient(param["host"], int(param["port"]))
        database_1 = connexion[param["instance"][target_1]["serveur"]]
        collection_1 = connexion[param["instance"][target_1]["collection"]]
        database_2 = connexion[param["instance"][target_2]["serveur"]]
        collection_2 = connexion[param["instance"][target_2]["collection"]]
        pipeline = [{
            "$match":{"operationType": opType}
        }]
        print(pipeline)
        print(database_1)
        print(database_2)
        print(collection_1)
        print(collection_2)
        with collection_1.watch(pipeline) as stream:
            for change in stream:
                result = change["fullDocument"]
                data = result["Pays"]
                tag = collection_2.find_one({"Pays": data})
                if tag == None:# si aucune corrspondance dans la base2 (propre)=> on insert
                    # collection_2.insert_one(result)
                    print("nouvelle insertion")
                else:# Au cas contraire on met juste à jour la date de l'existant
                    # collection_2.update_one({ "Pays": data}, { "$set": {"Date": datetime.now()}})
                    print("Date mise à jour")

watch = IsisMongoWatcher()
watch.start(0, 1, "insert")
