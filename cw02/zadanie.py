import pymongo
from bson.son import SON
from bson.code import Code


def zada( client):
    return client['business'].aggregate([ { "$group": { "_id": "$city"}}, { "$sort": { "_id": 1}} ])

def zadb(client):
    return client['review'].aggregate([ 
        {'$addFields': { 'year': { '$year': {'$toDate': "$date"}}}},
        {'$match': {'year': {'$gte': 2011 } } },
        {'$group': { '_id': 'null', 'total': { '$sum': 1}}}, 
        {'$project': {'_id': 0}} 
        ])

def zadc(client):
    return client['business'].find({'open': False}, { 'name': 1, 'full_address': 1, 'stars': 1})

def zadd(client):
    return client['user'].find({ '$or': [{'votes.funny': 0}, {'votes.useful': 0}] }).sort([( 'name', 1)])

def zade(client):
    return client['tip'].aggregate([
        { '$match': { 'date': { '$regex': "^2012.*"}}},
        { '$group': { "_id": "$business_id", "count": { '$sum': 1}}},
        { '$sort': { "count": 1}}
	])

def zadf(client):
    return client['review'].aggregate([
        { '$group': { '_id': "$business_id", 'avg': { '$avg': "$stars" }}},
        { '$match': { 'avg': { '$gt': 4.0}}}])

def zadg(client):
    pass

if __name__ == '__main__':
    host = 'localhost'
    port =  27017
    db = 'KarolHamielecCzw1250'
    cl = pymongo.MongoClient( host, port)

    l2 = list(zada(cl[db]))
    print(l2)


