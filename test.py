import pymongo
from pymongo import MongoClient
import json
import os
import sys
from datetime import datetime
import asyncio

def getConfigFromFile(jsonFilePath):
        with open(jsonFilePath) as configFile:
            config = json.load(configFile)
            return config

class IsisMongoWatcher:
    target = 0
    tag = ""
    
    @classmethod
    async def IsisTargeting(cls, target):
        cls.target = target
        config = getConfigFromFile('isis-mongo-conf.json')
        param = config["DEV"]
        connexion = MongoClient(param["host"], int(param["port"]))
        database = connexion[param["instance"][target]["serveur"]]
        collection = database[param["instance"][target]["collection"]]
        return collection

    @classmethod
    async def IsisPipeline(cls, opType):
        cls.operationType = opType
        pipeline = [{
            "$match":{"operationType": opType}
        }]
        return pipeline

    @classmethod
    async def IsisWatch(cls, target, opType):
        cls.target = target
        cls.opType = opType
        collection = await IsisMongoWatcher.IsisTargeting(target)
        pipeline = await IsisMongoWatcher.IsisPipeline(opType)
        print(collection)
        with collection.watch(pipeline) as stream:
            while stream.alive:
                change = stream.try_next()
                if change is not None:
                    result = change["fullDocument"]
                    print(result)
                    continue
                await asyncio.sleep(0.001)
                ##
                #### bout de code qui permet de vérifier dans la base 2 pour éviter les doublons #####
                ###
                # data = result["Pays"]
                # tag = collection_2.find_one({"Pays": data})
                # if tag == None:# si aucune corrspondance dans la base2 (propre)=> on insert
                #     collection_2.insert_one(result)
                #     print("nouvelle insertion")
                # else:# Au cas contraire on met juste à jour la date de l'existant
                #     collection_2.update_one({ "Pays": data}, { "$set": {"Date": datetime.now()}})
                #     print("Date mise à jour")

watcher = IsisMongoWatcher()
watcher_1 = watcher.IsisWatch(0, "insert")
watcher_2 = watcher.IsisWatch(2, "insert")

async def main():
    start = await asyncio.gather(watcher_1, watcher_2)
    return start
asyncio.run(main())


