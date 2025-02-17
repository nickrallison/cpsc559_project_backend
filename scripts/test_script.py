import requests
import json


class StoredObject:
    def __init__(self, user_id, user_message_id, data):
        self.user_id = user_id
        self.user_message_id = user_message_id
        self.data = data

    def __dict__(self):
        return {
            'user_id': self.user_id,
            'user_message_id': self.user_message_id,
            'data': self.data
        }


# Test script to create some objects and print them
# Run with `python scripts/test_script.py`
def main():
    userId = 1
    url = f'http://localhost:8080/objects?userId={userId}'
    response = requests.get(url).json()

    if len(response) == 0:
        print('No objects found, creating some test objects:')
        for i in range(3):
                for j in range(3):
                    obj = StoredObject(i, j, f'data-{i}-{j}')
                    response = requests.post(url, json=obj.__dict__())
                    print(f"Created object: {obj.__dict__()}")
    else:
        print('Objects found:')

    for obj in response:
        print(obj)


if __name__ == '__main__':
    main()