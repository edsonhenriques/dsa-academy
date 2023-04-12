from pymongo import MongoClient
import datetime

conn = MongoClient('localhost', 27017)

post1 = {"codigo": "ID-9987725",
        "prod_name": "Geladeira",
        "marcas": ["brastemp", "consul", "elecrolux"],
        "data_cadastro": datetime.datetime.utcnow()}

db = conn.cadastrodb
collection = db.posts
post_id = collection.insert_one(post1)

post2 = {"codigo": "ID-2209876",
        "prod_name": "Televisor",
        "marcas": ["samsung", "panasonic", "lg"],
        "data_cadastro": datetime.datetime.utcnow()}

collection = db.posts
post_id = collection.insert_one(post2).inserted_id
collection.find_one({"prod_name": "Televisor"})

for post in collection.find():
    print(post)

print(db.name)
print(db.collection_names())
