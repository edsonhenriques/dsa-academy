import pymongo

client_con = pymongo.MongoClient()
print(client_con.database_names())

db = client_con.cadastrodb
print(db.collection_names())

#criando uma coleção
db.create_collection("mycollection")
print(db.collection_names())