import requests
import json

def main():

    url = 'http://localhost:8080/messages'
    response = requests.get(url)
    print(response.json())

if __name__ == '__main__':
    main()