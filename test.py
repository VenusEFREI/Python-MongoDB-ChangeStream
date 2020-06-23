import pymongo
from pymongo import MongoClient
import json
from datetime import datetime
import asyncio


def getConfigFromFile(jsonFilePath):
    """Récupération de fichier de configuration au format JSON.

    Parse les fichiers JSON.

    Args:
        jsonFilePath: fichier JSON.
    
    Returns:
        Un dictionnaire contenant les paramètres de connexion aux BDD.
        example:

        {
        "DEV": {
            "host": "localhost",
            "port": "27017",
            "instance": [
                { "serveur": "database_1", "collection": "events"},
                { "serveur": "database_2", "collection": "processed"},
                { "serveur": "database_3", "collection": "events"}
            ],
            "replicatSet": "rs0"
            }
        }
    """
    with open(jsonFilePath) as configFile:
        config = json.load(configFile)
        return config

class IsisMongoWatcher:
    """**Class statique** définissant le schéma d'un watcher MongoDB.

    Args:
        **target** (*int*, *requis*): Un nombre qui sera l'indice du tableau d'instances du fichier de configuration JSON.
        **tag**(*str*, *requis*): Un string qui est le type de modification que l'on souhaite écouter.
        Exemple:

        "insert" 

    """
    target = 0
    tag = ""
    
    @classmethod
    async def IsisTargeting(cls, target):
        """**Methode de classe** spécifiant au watcher la collection à laquelle elle doit s'abonner.

        Cette methode va permettre de cibler les collections avec juste l'attribut target en parametre.

        Returns:
        On obtient une connexion à la base données et la collection qui va avec
        Exemple:

        Collection(Database(MongoClient(host=['localhost:27017'], 
        document_class=dict, tz_aware=False, connect=True), 
        'database_1'), 
        'events')

        Args:
            cls: Classe python qui represente l'instance en cours.
            **target**: Attribut de classe spécifié plus haut.
        """
        cls.target = target
        config = getConfigFromFile('isis-mongo-conf.json')
        param = config["DEV"]
        connexion = MongoClient(param["host"], int(param["port"]))
        database = connexion[param["instance"][target]["serveur"]]
        collection = database[param["instance"][target]["collection"]]
        return collection

    @classmethod
    async def IsisPipeline(cls, opType):
        """**Methode de classe** spécifiant au watcher quel type d'opétation il doit tracker.

        Cette methode va permettre de choisir le type d'opération que l'on veut suivre dans la collection.

        Returns:
            Renvoi un tableau d'objet nécessaire à la fonction watch pour savoir quoi faire et retourner quoi.
            Exemple:

            pipeline = [{
            "$match":{"operationType": "insert"}
            }]

        Args:
            cls: Classe python qui represente l'instance en cours.
            **opType**: Attribut de classe spécifié plus haut. 
        """
        cls.operationType = opType
        pipeline = [{
            "$match":{"operationType": opType}
        }]
        return pipeline

    @classmethod
    async def IsisWatch(cls, target, opType):
        """**Methode de classe** qui va s'abonner à une collection.

        Returns:
            Le watcher renvoi une notification sous forme de dictionnaire qu'on peut filtrer.
            Exemple:

            {'_id': 
            {'_data': '825EF21B7C000000012B022C0100296E5A10045292AE08149649959010ADEDFF3E080046645F696400645EF21B7CC9A02262599B0A790004'}, 
            'operationType': 'insert', 'clusterTime': Timestamp(1592925052, 1), 'fullDocument': 
            {'_id': ObjectId('5ef21b7cc9a02262599b0a79'), 
            'Pays': 'Afghanistan', 'Capital': 'Kabul', 'Population': '29,121,286', 'Date': datetime.datetime(2020, 6, 23, 17, 10, 52, 104000)}, 
            'ns': {'db': 'database_3', 'coll': 'events'}, 'documentKey': {'_id': ObjectId('5ef21b7cc9a02262599b0a79')}
            }

        Args:
            cls: Classe python qui represente l'instance en cours.
            **opType**: Attribut de classe spécifié plus haut. 
            **target**: Attribut de classe spécifié plus haut.
        """
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
                    print(change)
                    print(result)
                    continue
                await asyncio.sleep(0.001)
                
watcher = IsisMongoWatcher()
"""**Intanciation de la classe**.

usage:
    On cré une instance de la classe pour mettre en place un watcher 
    qui va s'abonner.
    Exemple:

    watcher_1 = watcher.IsisWatch(0, "insert")
"""

async def main():
    """**Fonction asynchrone main**"""
    start = await asyncio.gather(#****Instance de IsisMongoWatcher****)
    return start
asyncio.run(main())


