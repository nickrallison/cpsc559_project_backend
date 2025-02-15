import requests
import json

def main():

    url = 'localhost:8080/messages'
    # Make a GET request to get all messages
    response = requests.get(url)