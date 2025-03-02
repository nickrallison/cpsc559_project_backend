import requests


class StoredObject:
    def __init__(self, user_id, user_message_id, data):
        self.user_id = user_id
        self.user_message_id = user_message_id
        self.data = data

    def as_dict(self):
        return {
            "user_id": self.user_id,
            "user_message_id": self.user_message_id,
            "data": self.data
        }


# Test script to create some objects and print them
# Run with `python scripts/test_script.py`
def main():
    userId = 1
    url = f'http://localhost:8080/objects?userId={userId}'


    objects = []
    for i in range(3):
            for j in range(3):
                obj = StoredObject(i, j, f'data-{i}-{j}')
                objects.append(obj)
                print(f"Created object: {obj.as_dict()}")

    requests.post('http://localhost:8080/objects', json=[obj.as_dict() for obj in objects])
    response = requests.get(url).json()

    for obj in response:
        print(obj)


if __name__ == '__main__':
    main()