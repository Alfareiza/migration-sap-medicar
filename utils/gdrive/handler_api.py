from __future__ import print_function

import csv
import time
from datetime import datetime
import io
import os.path
import tempfile
from dataclasses import dataclass
from functools import lru_cache
from time import sleep

import chardet as chardet
from decouple import config
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload
from tenacity import retry, stop_after_attempt, wait_random

from core.settings import logger as log
from utils.decorators import ignore_unhashable, logtime, retry_until_true
from utils.resources import get_fibonacci_sequence

NUMBER_OF_ATTEMPTS = 7


@dataclass
class GDriveHandler:
    folders_ids = {}  # Recibe valores a medida que se consultan carpetas en func get_folder_id_by_name

    def __post_init__(self):
        # self.creds = Credentials.from_authorized_user_file('utils/gdrive/token.json', SCOPES)
        info = dict(token=config('TOKEN'),
                    refresh_token=config('REFRESH_TOKEN'),
                    client_id=config('CLIENT_ID'),
                    client_secret=config('CLIENT_SECRET'),
                    expiry=config('EXPIRY'),
                    scopes=['https://www.googleapis.com/auth/drive'])
        self.creds = Credentials.from_authorized_user_info(info)
        self.service = build('drive', 'v3', credentials=self.creds)
        self.files = []  # Se le agregan los archivos al ser ejecutada la función discover_files()

    @ignore_unhashable
    @lru_cache()
    def discover_folder_id_by_name(self, name) -> str:
        query = f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder'"
        response = self.service.files().list(q=query).execute()
        if folders := response.get('files', []):
            folder_id = folders[0]['id']
            GDriveHandler.folders_ids[name] = folders[0]['id']
            return folder_id
        log.warning(f'No fue encontrado folder {name} en Google Drive')
        return ''

    def get_folder_id_by_name(self, name) -> str:
        """Reach id of folder in attr of class"""
        folder_id = GDriveHandler.folders_ids.get(name)
        return folder_id or self.discover_folder_id_by_name(name)


    def get_files_in_folder_by_id(self, folder_id, ext='', only_files=True) -> list:
        """
        Reach the files of a given folder given an id.
        :param only_files: Define if only files will be reached or all the elements.
        :param ext: Extension of files to  be reached.
        :param folder_id: '1Pf...Y'
        :return: Ex.:
                    [
                        {'id': '123lkj...',
                        'name': 'Dispensacion-8.csv',
                        'createdTime': '2023-06-20T14:51:22.991Z',
                        'modifiedTime': '2023-06-10T20:26:35.000Z'},
                        {'id': '456mnb...',
                        'name': 'Dispensacion-7.csv',
                        'createdTime': '2023-06-20T14:51:24.290Z',
                        'modifiedTime': '2023-06-10T15:53:34.000Z'},
                        {'id': '345pokjn...',
                        'name': 'Dispensacion-6.csv',
                        'createdTime': '2023-06-20T14:51:24.290Z',
                        'modifiedTime': '2023-06-07T22:11:25.000Z'},
                        {'id': '1m...1A',
                        'name': 'convenioArticulos062023.csv',
                        'createdTime': '2023-06-05T13:00:48.698Z',
                        'modifiedTime': '2023-06-05T13:00:48.698Z'}
                    ]
        """
        query = f"'{folder_id}' in parents and trashed = false"
        if only_files:
            query += "and mimeType != 'application/vnd.google-apps.folder'"
        if ext:
            query += f" and fileExtension='{ext}'"
        fields = 'files(id, name, modifiedTime, createdTime, parents)'
        response = self.service.files().list(q=query, fields=fields).execute()
        files = response.get('files', [])
        return self.order_files_asc(files)

    @ignore_unhashable
    @lru_cache()
    @retry_until_true(3, 30)
    # @logtime('')
    def get_files_in_folder_by_name(self, folder_name, ext=None) -> list:
        """
        Reach the files of a folder given a folder name.
        :param ext: Extension of files to  be reached.
        :param filter: Parameter who help to filter the result before return it.
                       It might have the next estructure:
                       filter = {'ext': 'csv'}
        :param folder_name: 'My Folder'
        :return: Ex.: An empty list or something like the next:
                      [
                        {'id': '1Pf...Y',
                        'kind': 'drive#file',
                        'mimeType': 'application/vnd.google-apps.folder',
                        'name': 'AjustesSalidaMedicar'},
                       {'id': '1Pf...Bf',
                        'kind': 'drive#file',
                        'mimeType': 'application/vnd.google-apps.folder',
                        'name': 'AjustesEntradaMedicar'},
                       {'id': '1Pf...8',
                        'kind': 'drive#file',
                        'mimeType': 'application/vnd.google-apps.folder',
                        'name': 'DonacionesMedicar'},
                       {'id': '1Pf...f',
                        'kind': 'drive#file',
                        'mimeType': 'application/vnd.google-apps.folder',
                        'name': 'TrasladosMedicar'},
                       {'id': '1Pf...FR',
                        'kind': 'drive#file',
                        'mimeType': 'application/vnd.google-apps.folder',
                        'name': 'FacturacionMedicar'},
                       {'id': '1Pf...2',
                        'kind': 'drive#file',
                        'mimeType': 'application/vnd.google-apps.folder',
                        'name': 'DispensacionMedicar'},
                       {'id': '1Pf...z',
                        'kind': 'drive#file',
                        'mimeType': 'text/plain',
                        'name': 'any_file.csv',
                        'createdTime': '2023-06-05T13:00:48.698Z',
                        'modifiedTime': '2023-06-05T13:00:48.698Z'
                        }
                      ]
        """
        return self.get_files_in_folder_by_id(self.get_folder_id_by_name(folder_name), ext=ext)

    @logtime('DRIVE')
    def read_csv_file_by_id(self, file_id: str):
        # file_metadata = self.service.files().get(fileId=file_id).execute()

        # Download the file content as a bytes object
        request = self.service.files().get_media(fileId=file_id)
        file_content = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        # encoding = self.detect_csv_encoding(file_id)

        # Convert the bytes content to a string
        file_content_str = file_content.getvalue().decode('utf-8-sig')

        # Process the CSV file content using DictReader
        import csv
        from io import StringIO

        csv_data = StringIO(file_content_str)
        return csv.DictReader(csv_data, delimiter=';')

    def move_file(self, file: dict, to_folder_name: str) -> None:
        """
        Move a file to another folder.
        When it happens, might return a dict like this:
        {'kind': 'drive#file', 'id': '123kjkafF', 'name': 'file_name.csv',
        'mimeType': 'text/plain', 'parents': ['1LodKp...bBf'],
        'createdTime': '2023-07-05T22:15:43.580Z',
        'modifiedTime': '2023-07-05T21:59:16.994Z'}
        :param file: Dict with unique identification of the file in Google Drive.
        :param to_folder_name: Name of the folder where it will be moved.
        """
        new_parent_id = self.get_folder_id_by_name(to_folder_name)
        previous_parents = ",".join(file.get('parents'))
        self.service.files().update(
            fileId=file['id'], addParents=new_parent_id, removeParents=previous_parents, fields='id, parents'
        ).execute()

    def create_csv_in_drive(self, csv_to_dict, filename, folder_name, filter='') -> None:
        """
        Create a csv file in Google Drive considering the errors detected in the previous process.
        :param csv_to_dict: Csv2Dict
        :param filename: Name of the file.
        :param folder_name: Name of the folder where the file will be placed.
        :param filter:
        :return:
        """
        folder_id = self.get_folder_id_by_name(folder_name)
        rows = []
        for k, v in csv_to_dict.data.items():
            if filter == 'error' and k in csv_to_dict.errs or 'sin' in k:
                rows.extend(v['csv'])
            elif filter == '':
                rows.extend(v['csv'])
        if rows:
            fieldnames = rows[0].keys()
            with tempfile.NamedTemporaryFile(mode='w', delete=True) as temp_file:
                writer = csv.DictWriter(temp_file, fieldnames=fieldnames, delimiter=';')
                writer.writeheader()
                writer.writerows(rows)

                with open(temp_file.name, 'rb') as file:
                    media = MediaIoBaseUpload(io.BytesIO(file.read()),
                                              mimetype='text/csv', resumable=True)

                file_metadata = {'name': filename, 'parents': [folder_id]}

                file = self.service.files().create(body=file_metadata,
                                                   media_body=media,
                                                   fields='id').execute()
                log.info(f"CSV {filename!r} creado en carpeta {folder_name!r} con ID: {file['id']}")

    @retry(stop=stop_after_attempt(3), wait=wait_random(min=10, max=20))
    def prepare_and_send_csv(self, path_csv, filename, folder_name) -> None:
        """
        From a filepath, it sends the file to Google Drive.
        :param path_csv: '/Users/alfonso/Projects/SAPIntegration/dispensacion_processed.csv'
        :param filename: Name of the file.
        :param folder_name: Name of the folder where the file will be placed.
        :return:
        """
        log.info(f"Preparando envio de csv para GDrive de {filename!r}")
        folder_id = self.get_folder_id_by_name(folder_name)
        file_metadata = {'name': filename, 'parents': [folder_id]}
        media = MediaFileUpload(path_csv, mimetype='text/plain')
        file = self.send_csv(file_metadata, media)
        log.info(f"CSV {filename!r} creado en carpeta {folder_name!r}")

    def send_csv(self, file_metadata, media):
        """Try to create a file in Google Drive.
        In case of error, it will wait for three intervals (56, 111, 167) of seconds.
        """
        fibonacci_sequence = get_fibonacci_sequence(NUMBER_OF_ATTEMPTS, 55)
        for attempt in range(NUMBER_OF_ATTEMPTS):
            try:
                return self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            except HttpError as e:
                log.error(f'ATTEMPT#{attempt + 1} Error while fetching Google API (sync) -> {str(e)}')
                time.sleep(fibonacci_sequence[attempt])
                error = e

            if attempt == NUMBER_OF_ATTEMPTS - 1:
                raise error
            time.sleep(fibonacci_sequence[attempt])

    def detect_csv_encoding(self, file_id):
        request = self.service.files().get_media(fileId=file_id)
        response = request.execute()

        content = response.decode('utf-8')  # Decode the content to a string
        detector = chardet.UniversalDetector()

        # Feed the content to the detector line by line
        for line in content.splitlines():
            detector.feed(line.encode('raw_unicode_escape'))
            if detector.done:
                break

        detector.close()
        return detector.result['encoding']

    def order_files_asc(self, lst_files):
        """
        Given a list of files, return the same list
        with the items ordered by createdTime. The first
        will be the older.
        :param lst_files:
                    Ex.:
                        [
                            {'id': '123lkj...',
                            'name': 'Dispensacion-8.csv',
                            'createdTime': '2023-06-20T14:51:22.991Z',
                            'modifiedTime': '2023-06-10T20:26:35.000Z'},
                            {'id': '456mnb...',
                            'name': 'Dispensacion-7.csv',
                            'createdTime': '2023-06-20T14:51:24.290Z',
                            'modifiedTime': '2023-06-10T15:53:34.000Z'},
                            {'id': '345pokjn...',
                            'name': 'Dispensacion-6.csv',
                            'createdTime': '2023-06-20T14:51:24.290Z',
                            'modifiedTime': '2023-06-07T22:11:25.000Z'},
                            {'id': '1m...1A',
                            'name': 'convenioArticulos062023.csv',
                            'createdTime': '2023-06-05T13:00:48.698Z',
                            'modifiedTime': '2023-06-05T13:00:48.698Z'}
                        ]
        :return:
        """
        return sorted(lst_files, key=lambda x: datetime.strptime(x['createdTime'], '%Y-%m-%dT%H:%M:%S.%fZ'))


def main():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secrets.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('drive', 'v3', credentials=creds)

        # Call the Drive v3 API
        results = service.files().list(
            pageSize=10, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            print('No files found.')
            return
        print('Files:')
        for item in items:
            print(u'{0} ({1})'.format(item['name'], item['id']))
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')


if __name__ == '__main__':
    # main()
    # ...
    g = GDriveHandler()
    id_ = g.get_folder_id_by_name('DispensacionMedicar')
    id__ = g.get_folder_id_by_name('DispensacionMedicar')
