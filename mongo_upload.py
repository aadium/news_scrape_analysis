import pymongo
import json
from urllib.parse import quote_plus

password = quote_plus('Thunderbolt1@')
client = pymongo.MongoClient(f"mongodb+srv://aadiumrani:{password}@news-cluster.u92fpi9.mongodb.net/?retryWrites=true&w=majority")
db = client['news-db']

with open('output.json') as file:
    data = json.load(file)

for obj in data:
    url = obj['url']
    collection_name = ''

    # Determine collection based on URL content
    if 'https://techcrunch.com/' in url:
        collection_name = 'techcrunch'
    elif 'https://venturebeat.com/' in url:
        collection_name = 'venturebeat'
    elif 'https://www.eu-startups.com/' in url:
        collection_name = 'eu_startups'

    collection = db[collection_name]
    collection.insert_one(obj)

client.close()