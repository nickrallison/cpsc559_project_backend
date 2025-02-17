import requests
import json

class StoredObject:
    def __init__(self, unix_millis, user_id, data):
        self.id = None
        self.unix_millis = unix_millis
        self.user_id = user_id
        self.data = data

    def __str__(self):
        return f'id: {self.id}, unix_millis: {self.unix_millis}, user_id: {self.user_id}, data: {self.data}'

    def __repr__(self):
        return f'id: {self.id}, unix_millis: {self.unix_millis}, user_id: {self.user_id}, data: {self.data}'

    def __dict__(self):
        return {
            'id': self.id,
            'unix_millis': self.unix_millis,
            'user_id': self.user_id,
            'data': self.data
        }

def main():

    url = 'http://localhost:8080/objects'

    # 100 users each created 100 objects

    for i in range(100):
        for j in range(100):
            obj = StoredObject(id=f'{i}-{j}', unix_millis=0, user_id=i, data=f'{i}-{j}')
            response = requests.post(url, json=obj.__dict__())
            print(response.json())

    response = requests.get(url)
    print(response.json())

if __name__ == '__main__':
    main()