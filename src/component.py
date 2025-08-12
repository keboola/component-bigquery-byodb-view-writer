import logging
import json
import re
from typing import List

from kbcstorage.client import Client
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from google_cloud.bigquery_client import BigqueryClient

# configuration variables
KEY_SERVICE_ACCOUNT = "#service_account"

KEY_BUCKETS = "source_bucket"
KEY_SOURCE_PROJECT_ID = "source_project_id"
KEY_SOURCE_TABLE_ID = "source_table_id"

KEY_DESTINATION_PROJECT_ID = "destination_project_id"
KEY_DESTINATION_DATASET_ID = "destination_dataset_id"
KEY_DESTINATION_VIEW_NAME = "destination_view_name"
KEY_CUSTOM_COLUMNS = "custom_columns"
KEY_COLUMNS = "columns"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [
    KEY_SERVICE_ACCOUNT,
    KEY_SOURCE_PROJECT_ID,
    KEY_DESTINATION_PROJECT_ID,
    KEY_DESTINATION_DATASET_ID,
    KEY_DESTINATION_VIEW_NAME,
]

SCOPES = ["https://www.googleapis.com/auth/bigquery"]


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

    @staticmethod
    def validate_credentials(parameters):
        if not parameters:
            raise UserException("Authorization missing.")

        parameters = json.loads(parameters)

        private_key = parameters.get("private_key")
        if private_key == "" or private_key is None:
            raise UserException("Service account private key missing.")

        client_email = parameters.get("client_email")
        if client_email == "" or client_email is None:
            raise UserException("Service account client email missing.")

        token_uri = parameters.get("token_uri")
        if token_uri == "" or token_uri is None:
            raise UserException("Service account token URI missing.")

        return parameters

    def get_bigquery_credentials(self):
        credentials = self.configuration.config_data.get("image_parameters", {}).get(
            KEY_SERVICE_ACCOUNT
        ) or self.configuration.parameters.get(KEY_SERVICE_ACCOUNT)

        credentials_json = self.validate_credentials(credentials)

        try:
            return BigqueryClient.get_service_account_credentials(
                credentials_json, SCOPES
            )
        except ValueError as err:
            message = 'Cannot get credentials from service account %s. Reason "%s".' % (
                credentials_json.get("client_email"),
                str(err),
            )
            raise UserException(message)

    def run(self):
        """
        Main execution code
        """
        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        # check if source BigQuery project is actual KBC project
        if self._get_kbc_project_id() not in params.get(KEY_SOURCE_PROJECT_ID):
            raise UserException("Another project storage backend is not supported!")

        # create bigquery client
        bg = BigqueryClient(self.get_bigquery_credentials())

        # get destination datasets
        destination_dataset = bg.find_dataset(
            params.get(KEY_DESTINATION_PROJECT_ID),
            params.get(KEY_DESTINATION_DATASET_ID),
        )
        if params.get(KEY_SOURCE_TABLE_ID):
            # expand and unify KBC table id to dataset and table (in.c-test.Account > in_c_test, Account)
            source_dataset, source_table = self.expand_table_id(
                params.get(KEY_SOURCE_TABLE_ID)
            )

            sapi_client = Client(self._get_kbc_root_url(), self._get_storage_token())

            tables = sapi_client.tables.detail(params.get(KEY_SOURCE_TABLE_ID))

            source_table_columns_descriptions = self._get_fields_descriptions(tables)

            # get destination datasets
            source_dataset = bg.find_dataset(
                params.get(KEY_SOURCE_PROJECT_ID), source_dataset
            )

            custom_columns = (
                params.get(KEY_COLUMNS) if params.get(KEY_CUSTOM_COLUMNS) else None
            )
            # add check if dataset exist is in the same region
            if source_dataset.location != destination_dataset.location:
                raise Exception(
                    "Source and destination datasets are in different locations! View creation is not supported."
                )
            else:
                logging.info(
                    f"Source and destination datasets are in the same location: {source_dataset.location}"
                )

            # create view
            bg.create_view(
                destination_dataset,
                source_dataset,
                source_table,
                source_table_columns_descriptions,
                custom_columns,
                params.get(KEY_DESTINATION_VIEW_NAME),
            )
        else:
            # delete view if exists
            logging.warning("No source table selected. Deleting view if exists.")
            bg.delete_view(destination_dataset, params.get(KEY_DESTINATION_VIEW_NAME))

    @staticmethod
    def _get_fields_descriptions(tables):
        fields_descriptions = {}
        columns_metadata = tables.get("columnMetadata")
        for column_name, metadata in columns_metadata.items():
            for item in metadata:
                if "KBC.description" == item.get("key"):
                    fields_descriptions[column_name] = item.get("value")
        return fields_descriptions

    @staticmethod
    def expand_table_id(table_id):
        # in.c-test.Account > in_c_test, Account
        split = table_id.split(".")
        stage = split[0]
        bucket = split[1].replace("-", "_")
        return f"{stage}_{bucket}", split[2]

    def _get_kbc_root_url(self):
        return f"https://{self.environment_variables.stack_id}"

    def _get_kbc_project_id(self):
        return self.environment_variables.project_id

    def _get_storage_token(self) -> str:
        return (
            self.configuration.parameters.get("#storage_token")
            or self.environment_variables.token
        )

    @sync_action("get_buckets")
    def get_buckets(self) -> List[SelectElement]:
        """
        Sync action for getting list of available buckets
        Returns:

        """
        sapi_client = Client(self._get_kbc_root_url(), self._get_storage_token())

        buckets = sapi_client.buckets.list()
        return [
            SelectElement(value=b["id"], label=f"{b['id']} ({b['name']})")
            for b in buckets
        ]

    @sync_action("get_tables")
    def get_tables(self) -> List[SelectElement]:
        """
        Sync action for getting list of available tables
        Returns:

        """
        bucket = self.configuration.parameters.get(KEY_BUCKETS)
        if not bucket:
            raise UserException("No bucket selected.")
        sapi_client = Client(self._get_kbc_root_url(), self._get_storage_token())

        tables = sapi_client.tables.list()
        filtered_tables = [t for t in tables if t["bucket"]["id"] == bucket]
        return [
            SelectElement(value=t["id"], label=f"{t['displayName']} ({t['name']})")
            for t in filtered_tables
        ]

    @sync_action("get_columns")
    def get_columns(self) -> List[SelectElement]:
        """
        Sync action for getting list of available columns
        Returns:

        """
        table_id = table = self.configuration.parameters.get(KEY_SOURCE_TABLE_ID)
        if not table:
            raise UserException("No table selected.")
        sapi_client = Client(self._get_kbc_root_url(), self._get_storage_token())

        table = sapi_client.tables.detail(table_id)
        return [SelectElement(value=c, label=c) for c in table.get("columns", [])]

    @sync_action("get_source_projects")
    def get_source_projects(self) -> List[SelectElement]:
        """
        Sync action for getting list of available projects
        Returns:

        """
        bq = BigqueryClient(credentials=self.get_bigquery_credentials())
        projects = bq.client.list_projects()
        kbc_project_id = self._get_kbc_project_id()

        # source project can be only the current KBC project
        project_pattern = re.compile(rf'kbc-grpn-{kbc_project_id}-')

        projects = [
            SelectElement(
                value=p.project_id, label=f"{p.project_id} ({p.friendly_name})"
            )
            for p in projects
            if project_pattern.search(p.project_id)
        ]
        if len(projects) == 0:
            raise UserException(
                "No projects found. You cannot have access to any project or list projects."
            )
        return projects

    @sync_action("get_destination_projects")
    def get_destination_projects(self) -> List[SelectElement]:
        """
        Sync action for getting list of available projects
        Returns:

        """
        bq = BigqueryClient(credentials=self.get_bigquery_credentials())
        projects = bq.client.list_projects()
        kbc_project_id = self._get_kbc_project_id()
        # destination project CAN'T be the current KBC project
        project_pattern = re.compile(rf'kbc-grpn-{kbc_project_id}-')

        projects = [
            SelectElement(
                value=p.project_id, label=f"{p.project_id} ({p.friendly_name})"
            )
            for p in projects
            if not project_pattern.search(p.project_id)
        ]
        return projects

    @sync_action("get_datasets")
    def get_datasets(self) -> List[SelectElement]:
        """
        Sync action for getting list of available datasets
        Returns:

        """
        bq_project = self.configuration.parameters.get(KEY_DESTINATION_PROJECT_ID)
        if not bq_project:
            raise UserException("No project selected.")
        bq = BigqueryClient(credentials=self.get_bigquery_credentials())
        return [
            SelectElement(value=d.dataset_id, label=f"{d.dataset_id}")
            for d in bq.client.list_datasets(bq_project)
        ]


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
