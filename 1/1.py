from bson.json_util import dumps
import msgpack
import pymongo
import json


def write_json(filename,data):
    with open(f'1/{filename}','w',encoding='utf-8') as f:
        json.dump(data,f,ensure_ascii=False,indent=2)

# Функции для чтения файла формата msgpack
def load_msgpack(file_path):
    with open(file_path, 'rb') as file:
        data = msgpack.unpack(file)
    return data


def insert_to_mongo(data, db_name, collection_name):
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

    return collection


def query_1(collection):
    """вывод первых 10 записей, отсортированных по убыванию по полю salary"""
    result = collection.find().sort('salary', pymongo.DESCENDING).limit(10)
    return dumps(result, indent=4)


def query_2(collection):
    result = collection.find({'age': {'$lt': 30}}).sort('salary', pymongo.DESCENDING).limit(15)
    return dumps(result, indent=4)


def query_3(collection, city, job: list):
    result = collection.find({
        'city': city,
        'job': {'$in': job}
    }).sort('age', pymongo.ASCENDING).limit(10)
    return dumps(result, indent=4)


def query_4(collection, age_range, year_range, salary_ranges):
    result = collection.count_documents({
        'age': {'$gte': age_range[0], '$lte': age_range[1]},
        'year': {'$gte': year_range[0], '$lte': year_range[1]},
        '$or': [
            {'salary': {'$gt': salary_ranges[0][0], '$lte': salary_ranges[0][1]}},
            {'salary': {'$gt': salary_ranges[1][0], '$lte': salary_ranges[1][1]}}
        ]
    })
    return result


def main():
    file_path = 'data/task_1_item.msgpack'
    db_name = "data_eng"
    collection_name = "work_5"
    data = load_msgpack(file_path)
    collection = insert_to_mongo(data,db_name,collection_name)
    result = {
        "query_1": json.loads(query_1(collection)),
        "query_2": json.loads(query_2(collection)),
        "query_3": json.loads(query_3(collection, 'Рига', ['Косметолог', 'Медсестра', 'Архитектор'])),
        "query_4": query_4(collection, (25, 40), (2019, 2022), [(50000, 75000), (125000, 150000)])
    }

    # Сохраняем результат в result.json
    with open('1/result.json','w',encoding='utf-8') as f:
        json.dump(result,f,ensure_ascii=False,indent=2)


if __name__ == '__main__':
    main()

