import logging

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound


class BigqueryClient:
    @staticmethod
    def get_service_account_credentials(service_account_info, scopes):
        return service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=scopes
        )

    def __init__(self, credentials, location):
        self.client = bigquery.Client(credentials=credentials, location=location)

    def _update_access(self, source_dataset, view):
        access_entries = source_dataset.access_entries
        access_entry = bigquery.AccessEntry(None, "view", view.reference.to_api_repr())

        if access_entry not in access_entries:
            access_entries.append(access_entry)
            source_dataset.access_entries = access_entries
            self.client.project = source_dataset.project
            self.client.update_dataset(source_dataset, ["access_entries"])
            logging.info(f"View {view.reference.to_api_repr()} has been granted access to the source dataset.")
        else:
            logging.info(
                f"View {view.reference.to_api_repr()} already has access to the source dataset. ({access_entry})")

    def find_dataset(self, project_id, dataset_id):
        dataset_id_full = f"{project_id}.{dataset_id}"
        try:
            logging.info(f"Getting dataset {dataset_id_full}")
            self.client.project = project_id
            return self.client.get_dataset(dataset_id_full)
        except NotFound:
            raise Exception(f"Dataset {dataset_id_full} does not exist.")

    def _get_view(self, dataset, view_id):
        try:
            self.client.project = dataset.project
            return self.client.get_table(dataset.table(view_id))
        except NotFound:
            logging.info(f"Table or view {view_id} is not found in dataset {dataset.dataset_id} ({dataset.project})")
            return bigquery.Table(dataset.table(view_id))

    @staticmethod
    def _compose_view_query(source_project, source_dataset, source_table_id, destination_dataset, view_name,
                            custom_columns):
        if custom_columns:
            return f"""
                    CREATE OR REPLACE VIEW `{destination_dataset}`.`{view_name}` AS
                      SELECT
                          {', '.join([f'`{col}`' for col in custom_columns])}
                      FROM
                          `{source_project}`.`{source_dataset.dataset_id}`.`{source_table_id}`
                  """
        return f"""
                CREATE OR REPLACE VIEW `{destination_dataset}`.`{view_name}` AS
                  SELECT
                      *
                  FROM
                      `{source_project}`.`{source_dataset.dataset_id}`.`{source_table_id}`
              """

    def create_view(self, destination_dataset, source_dataset, source_table, custom_columns, view_name):

        query = self._compose_view_query(source_dataset.project, source_dataset, source_table, custom_columns)

        self.client.project = destination_dataset.project

        job = self.client.query(query)

        logging.info(
            f"View {job.destination.project}.{job.destination.dataset_id}.{job.destination.table_id} has been created.")

        created_view = self._get_view(destination_dataset, view_name)
        self._update_access(source_dataset, created_view)
