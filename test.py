import requests
headers = {"Authorization": f"Bearer {self.credentials.token}"}
response = requests.get("https://doubleclickbidmanager.googleapis.com/v2/queries", headers=headers)
print(response.status_code, response.text)
