from apiclient import discovery
from oauth2client import client, tools
from oauth2client.file import Storage
from pprint import pprint
import argparse
import httplib2
import os


class GoogleSheet:

    _SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    _CLIENT_SECRET_FILE = 'client_secret.json'
    _APPLICATION_NAME = 'Google Sheets API Python'
    _DISCOVERY_URL = 'https://sheets.googleapis.com/$discovery/rest?version=v4'

    def __init__(self, spreadsheetId):

        credentials = self._get_credentials()
        http = credentials.authorize(httplib2.Http())

        self._service = discovery.build('sheets', 'v4', http=http,
                                        discoveryServiceUrl=self._DISCOVERY_URL)
        self._spreadsheetId = spreadsheetId

    def _get_credentials(self):
        """Gets valid user credentials from storage.

        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.

        Returns:
            Credentials, the obtained credential.
        """
        # home_dir = os.path.expanduser('~')
        # credential_dir = os.path.join(home_dir, '.credentials')
        credential_dir = os.path.abspath('.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir,
                                       'sheets.googleapis.com-python.json')

        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(self._CLIENT_SECRET_FILE,
                                                  self._SCOPES)
            flow.user_agent = self._APPLICATION_NAME
            flags = (argparse
                     .ArgumentParser(parents=[tools.argparser])
                     .parse_args(['--noauth_local_webserver']))
            if flags:
                credentials = tools.run_flow(flow, store, flags)
            else:  # Needed only for compatibility with Python 2.6
                credentials = tools.run(flow, store)
            # print('Storing credentials to ' + credential_path)
        return credentials

    def get_values(self, rangeName):
        result = self._service.spreadsheets().values().get(
            spreadsheetId=self._spreadsheetId, range=rangeName).execute()
        values = result.get('values', [])
        return values

    def set_values(self, rangeName, values):
        body = {
            'values': values
        }
        result = self._service.spreadsheets().values().update(
            spreadsheetId=self._spreadsheetId, range=rangeName,
            valueInputOption='USER_ENTERED', body=body).execute()


if __name__ == '__main__':
    # gs = GoogleSheet('1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms')
    # pprint(gs.get_values('Class Data!A2:E'))

    gs = GoogleSheet('1j2nNxHfqEFnDC_qFu00ncCM7139gR5OxWMcs5ylXDMQ')
    # pprint(gs.get_values('Corrupted list (testing)!A:M'))
    gs.set_values('Corrupted list (testing)!A:M', [['']])
