import base64
import re
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from bs4 import BeautifulSoup
import email

# If modifying these SCOPES, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

def gmail_authenticate():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

def read_email(service, user_id, email_id):
    try:
        message = service.users().messages().get(userId=user_id, id=email_id, format='raw').execute()
        msg_str = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))
        mime_msg = email.message_from_bytes(msg_str)

        return mime_msg
    except Exception as error:
        print(f'An error occurred: {error}')

def send_email(service, user_id, recipient, subject, body):
    message = email.mime.text.MIMEText(body)
    message['to'] = recipient
    message['subject'] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes())
    raw = raw.decode()
    body = {'raw': raw}

    try:
        message = service.users().messages().send(userId=user_id, body=body).execute()
        print('Message Id: %s' % message['id'])
        return message
    except Exception as error:
        print(f'An error occurred: {error}')

def parse_table_from_email(email_content):
    soup = BeautifulSoup(email_content, 'html.parser')
    tables = soup.find_all('table')
    data = []

    for table in tables:
        rows = table.find_all('tr')
        headers = [header.text.strip() for header in rows[0].find_all('th')]

        for row in rows[1:]:
            cells = row.find_all('td')
            row_data = {headers[i]: cell.text.strip() for i, cell in enumerate(cells)}
            data.append(row_data)

    return data

# Example usage
service = gmail_authenticate()
user_id = 'me'  # 'me' indicates the authenticated user

# Read an email (replace 'EMAIL_ID' with the actual email ID)
email_content = read_email(service, user_id, 'EMAIL_ID')
parsed_data = parse_table_from_email(email_content.get_payload())

# Send an email
send_email(service, user_id, 'recipient@example.com', 'Subject here', 'Body here')
