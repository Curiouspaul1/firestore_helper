from google.cloud import firestore
from google.cloud.firestore_v1 import base_client
from google.oauth2 import service_account
from typing import Dict, List
import os
import json
import datetime as dt
import  uuid

base_dir = os.path.abspath(os.getcwd())

# instantiate firestore client

credentials = service_account.Credentials.from_service_account_file(
    f'{base_dir}/config.json'
)

db = firestore.Client(credentials=credentials)


_TYPES = [int, str, float, dt.datetime]

def add_from_object(data: Dict, collection_name: str) -> Dict:
    """
        use this to add json objects to a firestore collection
    """
    try:
        db.collection(u'{}'.format(collection_name)).document(
            str(uuid.uuid4())
        ).set(data)

        return {
            "status": "success"
        }
    except Exception as e:
        return {
            "status": "Error",
            "msg": repr(e)
        }

def add_from_file(path_to_file, collection_name) -> Dict:
    """
        specify a path to the file or if it exists in the same directory
        as this script simply pass in its name. The path should include
        the file extension, which should be '.json' .
    """

    data = None
    with open(path_to_file, 'r') as file:
        data = json.load(file)
    print(data)
    if type(data) == list:
        # if this is a list of json objects try to add them
        #  one by one
        count_ = 0
        commitCount = 0
        batches_ = []
        batches_.append(db.batch())
        for obj in data:
            if count_ <= 499:
                _insert = db.collection(u'{}'.format(collection_name)).document(
                str(uuid.uuid4())
                )
                batches_[commitCount].set(_insert, obj)
                count_ += 1
            else:
                print(count_)
                print(commitCount)
                count_ = 0
                commitCount += 1
                batches_.append(db.batch())
        for batch in batches_:
            batch.commit()
        return {
            "status": "success",
            "data": result
        }
    else:
        #  searches for an entry point to the list 
        #  of json objects in this case it assumes that
        #  a key of 'data' exists in the json file, that maps to an array of
        #  objects
        count_ = 0
        commitCount = 0
        batches_ = []
        batches_.append(db.batch())
        for obj in data['data']:
            if count_ <= 499:
                _insert = db.collection(u'{}'.format(collection_name)).document(
                str(uuid.uuid4())
                )
                batches_[commitCount].set(_insert, obj)
                count_ += 1
            else:
                print(count_)
                print(commitCount)
                count_ = 0
                commitCount += 1
                batches_.append(db.batch())
        for batch in batches_:
            batch.commit()
        return {
            "status": "success",
            "data": result
        }



def fetch_one(collection_name, **kwargs):
    """
        pass in a collection_name followed by the names of the fields you want to
        search by, as well as the search term for each field.
    """
    query = db.collection(u'{}'.format(collection_name))
    for key, value in kwargs.items():
        if type(value) in _TYPES:
            print(type(value) in _TYPES)
            query = query.where(
                u'{}'.format(key),
                u'==',
                value
            )
        else:
            query = query.where(
                u'{}'.format(key),
                u'==',
                u'{}'.format(value)
            )
    print(query)
    result = query.limit(1).get()[0].to_dict()
    return {
        "status": "success",
        "data": result
    }


def fetch_all(collection_name, **kwargs):
    """
        pass in a collection_name followed by the names of the fields you want to
        search by, as well as the search term for each field.
    """
    query = db.collection(u'{}'.format(collection_name))
    for key, value in kwargs.items():
        if type(value) in _TYPES:
            query = query.where(
                u'{}'.format(key),
                u'==',
                value
            )
        else:
            query = query.where(
                u'{}'.format(key),
                u'==',
                u'{}'.format(value)
            )
    data_ = query.get()
    result = []
    for obj in data_:
        result.append(obj.to_dict())
    return {
        "status": "success",
        "data": result
    }


# print(add_from_object(data={"name": "Adam", "age": 15}, collection_name="dummy"))
# print(fetch_one(collection_name='dummy', age=5))
# print(fetch_all(collection_name='dummy', age=5, name='Adam'))
print(add_from_file('test.json', collection_name='dummy'))