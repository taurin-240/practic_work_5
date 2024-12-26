import pymongo
import json
from bson.json_util import dumps

TOKEN = "384e8ff046cb40e1bb8b663adb9455b0"

def load_data(file_path):
    with open(file_path,'r',encoding='utf-8') as f:
        data = json.load(f)
    return data

def connect_mongo():
    host = 'localhost'
    port = 27017
    db_name: str = 'data_eng'
    collection: str = 'work_5_4'
    client = pymongo.MongoClient(f'mongodb://{host}:{port}')
    collection = client[db_name][collection]
    return collection

def insert_to_mongo(data, collection):
    if isinstance(data, list):
        collection.insert_many(data)
    elif isinstance(data, dict):
        collection.insert_one(data)

def query_1(collection):
    """вывод первых 10 записей, отсортированных по убыванию по полю salary"""
    result = collection.find().sort('publishedAt', pymongo.DESCENDING).limit(10)
    return dumps(result, indent=4)


def query_2(collection):
    """вывод первых 15 записей, отфильтрованных по предикату: есть в поле title текст AI или Finance"""
    query = {
    'title': {
        '$regex': r'\bAI\b|\bFinance\b',  # Ищем "AI" или "Finance" в поле title
        '$options': 'i'  # 'i' для игнорирования регистра
    }
}
    result = collection.find(query).sort('publishedAt', pymongo.DESCENDING).limit(15)
    return dumps(result, indent=2)

def query_3(collection):
    """Запрос выводит минимум,максимум и среднее значение слов в поле discription"""
    query = {
        'title': {
            '$regex': r'\bAI\b|\bFinance\b',  # Ищем "AI" или "Finance" в поле title
            '$options': 'i'  # 'i' для игнорирования регистра
        }
    }
    pipeline = [
    {'$match': query },  # Можно добавить фильтрацию 
    {'$project': {  # Проекция для добавления поля num_words
        'num_words': {'$size': {'$split': ['$description', ' ']}}
    }},
    {'$group': {  # Группировка для расчета статистики
        '_id': None,
        'max_words': {'$max': '$num_words'},
        'avg_words': {'$avg': '$num_words'},
        'min_words': {'$min': '$num_words'}
    }}
    ]
    result = collection.aggregate(pipeline)
    return dumps(result,indent=2)


# Удаление записей по предикату
def query_4(collection, predicate):
    result = collection.delete_many(predicate)
    return {"deleted_count": result.deleted_count}


def main():
    file_path = 'data/news.json'
    data = load_data(file_path)
    collection = connect_mongo()
    insert_to_mongo(data['articles'],collection)

    results = {
        'query_1': json.loads(query_1(collection)),
        'query_2': json.loads(query_2(collection)),
        'query_3': json.loads(query_3(collection)),
        'query_4': query_4(collection,{ 'description': {'$regex':'Google'} })
    }
    with open('4/results.json','w',encoding='utf-8') as f:
        json.dump(results,f,ensure_ascii=False,indent=2)

if __name__ == '__main__':
    main()