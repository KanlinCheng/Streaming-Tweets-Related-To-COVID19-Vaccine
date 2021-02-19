import pymongo

client = pymongo.MongoClient("mongodb+srv://user:dsci551@cluster0.prlkd.mongodb.net/twitter_db?retryWrites=true&w=majority")
db = client.twitter_db
collection = db.collection0

data = {
    'title': 'Python and MongoDB',
    'content': 'PyMongo is fun, you guys',
    'author': 'Scott'
}

result = collection.insert_one(data)