import requests, msal, json

def getToken(tenant, client_id, client_secret):
    authority = "https://login.microsoftonline.com/" + tenant
    scope = ["https://api.businesscentral.dynamics.com/.default"]

    app = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential = client_secret)
    
    try:
      accessToken = app.acquire_token_for_client(scopes=scope)
      if accessToken['access_token']:
        print('New access token retreived....')
      else:
        print('Error aquiring authorization token.')
    except Exception as err:
      print(err)
    
    return accessToken

client_id =  ''
client_secret = ''
tenant = ''


token = getToken(tenant,client_id,client_secret)['access_token']


print(token)
headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

#url = "https://api.businesscentral.dynamics.com/v2.0/c261a61f-eb5d-407a-878c-c7502495b991/Production/api/v2.0/companies(777348ae-1293-ec11-80f1-0022488304f4)/generalLedgerEntries?$filter=lastModifiedDateTime gt 2025-03-01T00:00:00.000Z"
#response = requests.get(url, headers=headers)


url = "https://api.businesscentral.dynamics.com/v2.0/c261a61f-eb5d-407a-878c-c7502495b991/Production/api/v2.0/companies(c30c256a-1193-ec11-80f1-0022488304f4)/dimensionSetLine"
params = "{'$filter': 'lastModifiedDateTime gt 2025-03-01T00:00:00.000Z'}"
response = requests.get(url, headers=headers, params=params)



resultado = json.loads(response.content)
print(resultado)
print(len(resultado))
                    
