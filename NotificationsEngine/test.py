import json
import requests
import os

# Sandbox API URL format: https://api.mailgun.net/v3/sandbox&lt;ID&gt;.mailgun.org/messages
MAILGUN_API_URL = 'https://api.eu.mailgun.net/v3/mindanalytics.es/messages'
FROM_EMAIL_ADDRESS = "no-repy@mindanalytics.es"
apikey = os.getenv('MAILGUN_API_KEY', '')  # Set via environment variable
username = 'api'

def send_single_email(to_address: str, subject: str, message: str):
    try:
        data ={"from": FROM_EMAIL_ADDRESS, "to": to_address, "subject": subject, "text": message}
        resp = requests.post(MAILGUN_API_URL, auth=("api", apikey),data=data)
        if resp.status_code == 200:
            print(f"Successfully sent an email to '{to_address}' via Mailgun API.")
        else:
            print(f"Could not send the email, reason: {resp.text}")
    except Exception as ex:
        print(f"Mailgun error: {ex}")

if __name__ == "__main__":
    send_single_email("joaquin.orono@gmail.com", "Single email test", "Testing Mailgun API for a single email")
