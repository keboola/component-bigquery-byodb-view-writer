"""
Template Component main class.

"""
import logging
from typing import List

from kbcstorage.client import Client
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from google_cloud.bigquery_client import BigqueryClientFactory
from src.view_creator import ViewCreator

# configuration variables
KEY_SERVICE_ACCOUNT = 'service_account'

KEY_BUCKETS = 'source_buckets'
KEY_SOURCE_PROJECT_ID = 'source_project_id'
KEY_SOURCE_DATASET_ID = 'source_dataset_id'
KEY_SOURCE_TABLE_ID = 'source_table_id'

KEY_DESTINATION_PROJECT_ID = 'destination_project_id'
KEY_DESTINATION_DATASET_ID = 'destination_dataset_id'
KEY_DESTINATION_VIEW_ID = 'destination_view_id'
KEY_CUSTOM_COLUMNS = 'custom_columns'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_SERVICE_ACCOUNT, KEY_SOURCE_PROJECT_ID, KEY_SOURCE_DATASET_ID, KEY_SOURCE_TABLE_ID,
                       KEY_DESTINATION_PROJECT_ID, KEY_DESTINATION_DATASET_ID, KEY_DESTINATION_VIEW_ID]

SCOPES = ['https://www.googleapis.com/auth/bigquery']


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self.location = None
        self.project = None
        self.client = None
        self.credentials = None

    @staticmethod
    def validate_credentials(parameters):
        if not parameters:
            raise UserException('Authorization missing.')

        private_key = parameters.get('#private_key')
        if private_key == '' or private_key is None:
            raise UserException('Service account private key missing.')

        client_email = parameters.get('client_email')
        if client_email == '' or client_email is None:
            raise UserException('Service account client email missing.')

        token_uri = parameters.get('token_uri')
        if token_uri == '' or token_uri is None:
            raise UserException('Service account token URI missing.')

    def get_bigquery_credentials(self):
        credentials_json = (self.configuration.config_data.get('image_parameters', {}).get('service_account')
                            or self.configuration.parameters.get('service_account'))

        self.validate_credentials(credentials_json)

        credentials_json['private_key'] = credentials_json.get('#private_key')

        try:
            return BigqueryClientFactory.get_service_account_credentials(credentials_json, SCOPES)
        except ValueError as err:
            message = 'Cannot get credentials from service account %s. Reason "%s".' % (
                credentials_json.get('client_email'), str(err))
            raise UserException(message)

    def run(self):
        """
        Main execution code
        """
        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        source_project_id = params.get(KEY_SOURCE_PROJECT_ID)
        destination_project_id = params.get(KEY_DESTINATION_PROJECT_ID)

        dest_client = BigqueryClientFactory(destination_project_id, self.get_bigquery_credentials(),
                                            location=self.location).get_client()

        src_client = BigqueryClientFactory(source_project_id, self.get_bigquery_credentials(),
                                           location=self.location).get_client()

        view_creator = ViewCreator(dest_client, src_client,
                                   source_project_id,
                                   params.get(KEY_SOURCE_DATASET_ID),
                                   params.get(KEY_SOURCE_TABLE_ID),
                                   destination_project_id,
                                   params.get(KEY_DESTINATION_DATASET_ID),
                                   params.get(KEY_DESTINATION_VIEW_ID))
        view_creator.create_view()

    @staticmethod
    def expand_table_id(table_id):
        split = table_id.split('.')
        stage = split[0]
        bucket = split[1]
        table = split[2]
        dataset = f"{stage}_{bucket}"
        return dataset, table

    @sync_action('get_buckets')
    def get_available_buckets(self) -> List[SelectElement]:
        """
        Sync action for getting list of available buckets
        Returns:

        """
        sapi_client = Client(self._get_kbc_root_url(), self._get_storage_token())

        buckets = sapi_client.buckets.list()
        return [SelectElement(value=b['id'], label=f'({b["stage"]}) {b["name"]}') for b in buckets]

    @sync_action('get_tables')
    def get_available_tables(self) -> List[SelectElement]:
        """
        Sync action for getting list of available buckets
        Returns:

        """
        buckets = self._get_bucket_parameters()
        sapi_client = Client(self._get_kbc_root_url(), self._get_storage_token())

        tables = sapi_client.tables.list()
        filtered_tables = [t for t in tables if t['bucket']['id'] in buckets]
        return [SelectElement(value=t['id'], label=f'{t["displayName"]} ({t["name"]})') for t in filtered_tables]

    def _get_bucket_parameters(self):
        # TODO - implement better
        buckets = self.configuration.parameters.get(KEY_BUCKETS)
        if not buckets:
            raise UserException('No buckets selected.')
        return buckets

    def _get_kbc_root_url(self):
        return f'https://{self.environment_variables.stack_id}'

    def _get_storage_token(self) -> str:
        return self.configuration.parameters.get('#storage_token') or self.environment_variables.token


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
