import pymongo
from pymongo import MongoClient
import json
import os
import sys
from datetime import datetime

def getConfigFromFile(jsonFilePath):
        with open(jsonFilePath) as configFile:
            config = json.load(configFile)
            return config

class IsisMongoWatcher:
    target = 0
    tag = ""
    # def __init__(self, target, opType):
    #     self.target = target
    #     self.opType = opType

    @classmethod
    def IsisTargeting(cls, target):
        cls.target = target
        config = getConfigFromFile('isis-mongo-conf.json')
        param = config["DEV"]
        connexion = MongoClient(param["host"], int(param["port"]))
        database = connexion[param["instance"][target]["serveur"]]
        collection = database[param["instance"][target]["collection"]]
        return collection

    @classmethod
    def IsisPipeline(cls, opType):
        cls.operationType = opType
        pipeline = [{
            "$match":{"operationType": opType}
        }]
        return pipeline

    @classmethod
    def IsisWatch(cls, target, opType):
        cls.target = target
        cls.opType = opType
        collection = IsisMongoWatcher.IsisTargeting(target)
        pipeline = IsisMongoWatcher.IsisPipeline(opType)
        print(collection)
        print(pipeline)
        with collection.watch(pipeline) as stream:
            for change in stream:
                result = change["fullDocument"]
                # data = result["Pays"]
                print(result)
                # tag = collection_2.find_one({"Pays": data})
                # if tag == None:# si aucune corrspondance dans la base2 (propre)=> on insert
                #     collection_2.insert_one(result)
                #     print("nouvelle insertion")
                # else:# Au cas contraire on met juste à jour la date de l'existant
                #     collection_2.update_one({ "Pays": data}, { "$set": {"Date": datetime.now()}})
                #     print("Date mise à jour")

watcher_1 = IsisMongoWatcher()
watcher_1.IsisWatch(0, "insert")
# watcher_1 = IsisMongoWatcher(0, "insert")
# watcher_1.IsisWatch()
# IsisWatch(0, 1,"insert")
# print(target_1.IsisTargeting())
# print(target_1.IsisPipeline())
# target_1.IsisWatch(0,"insert")