import pymongo
import json

# Загрузка данных из msgpack файла и добавление их в коллекцию
def load_from_text(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read().strip().split('=====')
        for block in content:
            if not block.strip():
                continue
            fields = block.strip().split('\n')
            record = {}
            for field in fields:
                if '::' not in field:
                    continue
                key, value = field.split('::', 1)
                record[key.strip()] = value.strip()
            if 'job' in record:
                record['job'] = str(record['job'])
            if 'salary' in record:
                record['salary'] = int(record['salary'])
            if 'id' in record:
                record['id'] = int(record['id'])
            if 'city' in record:
                record['city'] = str(record['city'])
            if 'year' in record:
                record['year'] = int(record['year'])
            if 'age' in record:
                record['age'] = int(record['age'])

            data.append(record)
    return data

def insert_to_mongo(file_path, collection):
    data = load_from_text(file_path)
    if isinstance(data, list):
        collection.insert_many(data)
    elif isinstance(data, dict):
        collection.insert_one(data)

# Удаление по salary < 25 000 || salary > 175 000
def query_1(collection):
    result = collection.delete_many({"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]})
    return {"count": result.deleted_count}

# Увеличение возраста на 1
def query_2(collection):
    result = collection.update_many({}, {"$inc": {"age": 1}})
    return {"count": result.modified_count}

# повышение зарплаты на 5% для профессий
def query_3(collection, professions):
    result = collection.update_many({"job": {"$in": professions}}, {"$mul": {"salary": 1.05}})
    return {"count": result.modified_count}

# повышение зарплаты на 7% для городов
def query_4(collection, cities):
    result = collection.update_many({"city": {"$in": cities}}, {"$mul": {"salary": 1.07}})
    return {"count": result.modified_count}

# Повышение зарплаты на 10% по предикату
def query_5(collection, city, professions, age_range):
    result = collection.update_many({
        "$and": [
            {"city": city},
            {"job": {"$in": professions}},
            {"age": {"$gte": age_range[0], "$lte": age_range[1]}}
        ]
    }, {"$mul": {"salary": 1.10}})
    return {"count": result.modified_count}

# Удаление записей по предикату
def query_6(collection, predicate):
    result = collection.delete_many(predicate)
    return {"deleted_count": result.deleted_count}


def main():
    file_path = "data/task_3_item.text"
    db_name = "data_eng"
    collection_name = "work_5"

    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    results = {}

    insert_to_mongo(file_path, collection)

    results = {
        "query_1": query_1(collection),
        "query_2": query_2(collection),
        "query_3": query_3(collection, ["Продавец", "Учитель", "Повар"]),
        "query_4": query_4(collection, ["Бланес", "Тарраса"]),
        "query_5": query_5(collection, "Навалькарнеро", ["IT-специалист", "Инженер"], (30, 40)),
        "query_6": query_6(collection, {"salary": {"$lt": 30000}})
    }

    with open("3/3_results.json", "w", encoding='utf-8') as file:
        json.dump(results, file, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    main()