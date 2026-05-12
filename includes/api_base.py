import requests
import json

class ApiBase:
    ### Base class for the portal and facets API connections ###

    def __init__(self, api_url, api_token):
        self.api_url = api_url
        self.api_token = api_token

    def doPost(self, payload):
        headers = {
                  "Authorization": "Bearer " + self.api_token,
                  "Content-Type": "application/json"
              }
        response = requests.post(self.api_url, headers=headers, timeout=120, data=json.dumps(payload))
        
        try:
            return response.json()
        except json.JSONDecodeError as e:
            # print it out so we have it in the logs
            print(e.msg)
            print(response.text)
            return None

    def doGet(self, params):
        headers = {"Authorization": "Bearer " + self.api_token}
        response = requests.get(self.api_url, headers=headers, timeout=120, params=params)

        try:
            return response.json()
        except json.JSONDecodeError as e:
            # print it out so we have it in the logs
            print(e.msg)
            print(response.text)
            return None
    