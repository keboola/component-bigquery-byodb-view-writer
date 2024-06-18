import logging

from google.oauth2.credentials import Credentials
from google.cloud.bigquery import Client
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound


class BigqueryClientFactory(object):
    @staticmethod
    def get_service_account_credentials(service_account_info, scopes):
        return service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=scopes
        )

    def __init__(self, project: str, credentials: Credentials, location: str = 'US'):
        self.client = Client(
            project,
            credentials,
            location=location
        )

    def get_client(self):
        return self.client

    def get_dataset(self, project_id, dataset_id):
        dataset_id_full = f"{project_id}.{dataset_id}"
        try:
            logging.info(f"Getting dataset {dataset_id_full}")
            return self.client.get_dataset(dataset_id_full)
        except NotFound:
            raise Exception(f"Dataset {dataset_id_full} does not exist.")

    def get_view(self, view_id):
        try:
            return self.client.get_table(view_id)
        except NotFound:
            logging.info(f"Table {view_id} is not found.")
            return None
