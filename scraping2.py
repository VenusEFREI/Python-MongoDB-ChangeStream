import requests
from bs4 import BeautifulSoup
import time
import pymongo
from pymongo import MongoClient
from datetime import datetime

links = []

app = MongoClient('localhost', 27017)
db2 = app["database_3"]
collection2 = db2["events"]

with open('urls.txt', 'r') as file:
    for row in file:
        res = requests.get(row.strip())
        if res.ok:
            result = BeautifulSoup(res.text, 'html.parser')
            capital = result.find('tr', {'id': 'places_capital__row'}).find('td', {'class': 'w2p_fw'})
            pays = result.find('tr', {'id': 'places_country__row'}).find('td', {'class': 'w2p_fw'})
            population = result.find('tr', {'id': 'places_population__row'}).find('td', {'class': 'w2p_fw'})
            data = {"Pays": pays.text, "Capital": capital.text, "Population": population.text, "Date": datetime.now()}
            collection2.insert_one(data)
            print("Donnée insérée")
        time.sleep(2)###Délai pour éviter de spamer et rendre plus réaliste le scénario d'arrivé des events 