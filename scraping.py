import requests
from bs4 import BeautifulSoup
import time
import pymongo
from pymongo import MongoClient
from datetime import datetime

links = []

####Script qui récupère les URL de chaque pays en itérant dans une boucle
"""
for i in range(26):
    url = 'http://example.webscraping.com/places/default/index/' + str(i)
    res = requests.get(url)
    if res.ok:
        soup = BeautifulSoup(res.text, 'html.parser')
        tds = soup.findAll('td')
        for td in tds:
            a = td.find('a')
            link = a['href']
            links.append('http://example.webscraping.com' + link)
        time.sleep(3)
"""
######On inscrit URL par URL dans un txt
"""
with open('urls.txt', 'w') as file:
    for link in links:
        file.write(link + '\n')
"""
####Connexion à la base de données et à la collection
app = MongoClient('localhost', 27017)
db1 = app["database_1"]
collection1 = db1["events"]



####On récupère les informations qu'on veut et on les insère dans la collection

with open('urls.txt', 'r') as file:
    for row in file:
        res = requests.get(row.strip())
        if res.ok:
            result = BeautifulSoup(res.text, 'html.parser')
            capital = result.find('tr', {'id': 'places_capital__row'}).find('td', {'class': 'w2p_fw'})
            pays = result.find('tr', {'id': 'places_country__row'}).find('td', {'class': 'w2p_fw'})
            population = result.find('tr', {'id': 'places_population__row'}).find('td', {'class': 'w2p_fw'})
            data = {"Pays": pays.text, "Capital": capital.text, "Population": population.text, "Date": datetime.now()}
            collection1.insert_one(data)
            print("Donnée insérée")
        time.sleep(1)###Délai pour éviter de spamer et rendre plus réaliste le scénario d'arrivé des events 