from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import UserCreds, ClientCreds
import json
from .config import *
import os.path
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

async def get_service(name='gmail', version='v1'):
    async with Aiogoogle() as aiogoogle:
        return await aiogoogle.discover(name, version)

def convert_user_creds(user_creds):
    creds = UserCreds(
        access_token=user_creds.token,
        refresh_token=user_creds.refresh_token,
        expires_at=user_creds.expiry or None,
    )
    return creds

def convert_client_creds(filename):
    client_creds = {}
    with open(filename, 'r') as reader:
        data = reader.read()

        credentials = json.loads(data)
        client_creds = ClientCreds(
            client_id=credentials["installed"]["client_id"],
            client_secret=credentials["installed"]["client_secret"],
            scopes=DEFAULT_SCOPES
        )

    return client_creds


def get_token(filename='token.pickle',
              credentials="credentials.json",
              scopes=DEFAULT_SCOPES):
    creds = None
    if os.path.exists(filename):
        with open(filename, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials, scopes)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open(filename, 'wb') as token:
            pickle.dump(creds, token)

    return creds


def get_service(token):
    service = build('gmail', 'v1', credentials=token)
    return service