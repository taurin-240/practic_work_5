from bson.son import SON
from pymongo import MongoClient
import json
import pymongo

def connect_mongo():
    host = 'localhost'
    port = 27017
    db_name: str = 'data_eng'
    collection: str = 'work_5'
    client = MongoClient(f'mongodb://{host}:{port}')
    collection = client[db_name][collection]
    return collection

def load_data(file_path):
    with open(file_path,'r',encoding='utf-8') as f:
        data = json.load(f)
    return data

def insert_to_mongo(data, db_name, collection_name):
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    if isinstance(data, list):
        collection.insert_many(data)
    elif isinstance(data, dict):
        collection.insert_one(data)

def query_1(collection):
    #мин\сред\макс зп без сортировки
    result = collection.aggregate([
        {
            "$group": {
                '_id' : None,
                'min_salary' : {'$min': '$salary'},
                'avg_salary' : {'$avg' : '$salary'},
                'max_salary' : {'$max' : '$salary'}
            }
        }
    ])
    return list(result)[0] if result else {}

def query_2(collection):
    # количество меток профессий
    result = collection.aggregate([
        {
            '$group': {
                '_id' : 'job',
                "count" : {'$sum' : 1}
            }
        },
        {'$sort': SON([('_id', 1)])}
    ])
    return list(result)

def query_3(collection):
    # зп по городу
    result = collection.aggregate([
        {
             "$group": {
                "_id": "$city",
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"},
                "max_salary": {"$max": "$salary"}
            }
        },
        {"$sort": SON([("_id", 1)])}
    ])
    return list(result)


def query_4(collection):
    #зп по работе
    result = collection.aggregate([
        {
             "$group": {
                "_id": "$job",
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"},
                "max_salary": {"$max": "$salary"}
            }
        },
        {"$sort": SON([("_id", 1)])}
    ])
    return list(result)


def query_5(collection):
    #возраст по городу
    result = collection.aggregate([
        {
            "$group": {
                "_id": "$city",
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"},
                "max_age": {"$max": "$age"}
            }
        },
        {"$sort": SON([("_id", 1)])}
    ])
    return list(result)


def query_6(collection):
    #возраст по профессии
    result = collection.aggregate([
        {
            "$group": {
                "_id": "$job",
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"},
                "max_age": {"$max": "$age"}
            }
        },
        {"$sort": SON([("_id", 1)])}
    ])
    return list(result)


def query_7(collection):
    #макс зп при минимальном возрасте
    result = collection.aggregate([
        {
            "$group": {
                "_id": "$age",
                "max_salary": {"$max": "$salary"}
            }
        },
        {"$sort": SON([("_id", 1)])}
    ])
    return list(result)[0] if result else {}

def query_8(collection):
    # Мин зп при максимальном возрасте
    result = collection.aggregate([
        {
            "$group": {
                "_id": None,
                "max_age": {"$max": "$age"},
                "min_salary": {"$min": "$salary"}
            }
        }
    ])
    return list(result)[0] if result else {}

def query_9(collection):
    # Мин, сред, макс возраст по городу, если зп > 50 000
    result = collection.aggregate([
        {
            "$match": {"salary": {"$gt": 50000}}
        },
        {
            "$group": {
                "_id": "$city",
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"},
                "max_age": {"$max": "$age"}
            }
        },
        {
            "$sort": {"avg_age": -1}
        }
    ])
    return list(result)

def query_10(collection):
    # ЗП в нужном диапазоне по городу\профессии\возрасту
    ranges = [
        {"min_age": 18, "max_age": 25},
        {"min_age": 50, "max_age": 65}
    ]
    results = []
    for range_ in ranges:
        result = collection.aggregate([
            {
                "$match": {
                    "age": {"$gte": range_["min_age"], "$lt": range_["max_age"]}
                }
            },
            {
                "$group": {
                    "_id": {"city": "$city", "job": "$job"},
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"},
                    "max_salary": {"$max": "$salary"}
                }
            }
        ])
        results.extend(list(result))
    return results

def query_11(collection):
    # Произвольный запрос с $match, $group, $sort
    #ЗП по профессии для людей старше 30 лет по убыванию от средней
    result = collection.aggregate([
        {
            "$match": {"age": {"$gt": 30}}
        },
        {
            "$group": {
                "_id": "$job",
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"},
                "max_salary": {"$max": "$salary"}
            }
        },
        {
            "$sort": {"avg_salary": -1}
        }
    ])
    return list(result)


def main():
    collection = connect_mongo()
    file_path = 'data/task_2_item.json'
    db_name = 'data_eng'
    collection_name = 'work_5'
    data = load_data(file_path)
    insert_to_mongo(data, db_name, collection_name)

    results = {
        "query_1": query_1(collection),
        "query_2": query_2(collection),
        "query_3": query_3(collection),
        "query_4": query_4(collection),
        "query_5": query_5(collection),
        "query_6": query_6(collection),
        "query_7": query_7(collection),
        "query_8": query_8(collection),
        "query_9": query_9(collection),
        "query_10": query_10(collection),
        "query_11": query_11(collection)
    }

    with open("2/2_results.json", "w", encoding='utf-8') as file:
        json.dump(results, file, indent=2, ensure_ascii=False)


if __name__ == '__main__':
    main()