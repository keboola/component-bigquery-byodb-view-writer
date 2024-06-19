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
    def _compose_view_query(source_project, source_dataset, source_table_id, custom_columns):
        if custom_columns:
            return f"""
                      SELECT
                          {', '.join(custom_columns)}
                      FROM
                          `{source_project}`.`{source_dataset.dataset_id}`.`{source_table_id}`
                  """
        return f"""
                  SELECT
                      *
                  FROM
                      `{source_project}`.`{source_dataset.dataset_id}`.`{source_table_id}`
              """

    def create_view(self, destination_dataset, source_dataset, source_table, custom_columns, view_name):

        query = self._compose_view_query(source_dataset.project, source_dataset, source_table, custom_columns)

        self.client.project = destination_dataset.project

        view = self._get_view(destination_dataset, view_name)

        if not view.created:
            view_tmp = view
            view_tmp.view_query = query
            logging.info(f"Creating view {view_tmp.reference.to_api_repr()} with query {' '.join(query.split())}")
            created_view = self.client.create_table(view_tmp)
            logging.info(f"View {created_view.reference.to_api_repr()} has been created.")
            self._update_access(source_dataset, created_view)
        else:
            view.view_query = query
            logging.info(f"Updating view {view.reference.to_api_repr()} with query {' '.join(query.split())}")
            updated_view = self.client.update_table(view, ["view_query"])
            logging.info(f"View {updated_view.reference.to_api_repr()} has been updated.")
