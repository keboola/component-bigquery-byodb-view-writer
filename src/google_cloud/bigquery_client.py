import logging

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound


class BigqueryClient:
    @staticmethod
    def get_service_account_credentials(service_account_info, scopes):
        return service_account.Credentials.from_service_account_info(
            service_account_info, scopes=scopes
        )

    def __init__(self, credentials):
        self.client = bigquery.Client(credentials=credentials)

    def _update_access(self, source_dataset, view):
        access_entries = source_dataset.access_entries
        logging.info(f"Access entries before update: {access_entries}")

        new_access_entry = bigquery.AccessEntry(
            role=None,
            entity_type="view",
            entity_id={
                "projectId": view.reference.project,
                "datasetId": view.reference.dataset_id,
                "tableId": view.reference.table_id,
            },
        )

        if not any(
            entry.entity_type == "view"
            and entry.entity_id.get("projectId") == view.reference.project
            and entry.entity_id.get("datasetId") == view.reference.dataset_id
            and entry.entity_id.get("tableId") == view.reference.table_id
            for entry in access_entries
        ):
            access_entries.append(new_access_entry)
            source_dataset.access_entries = access_entries
            logging.info(
                f"View {view.reference} has been granted access to the source dataset"
            )
        else:
            logging.info(
                f"View {view.reference} already has access to the source dataset"
            )

        self.client.project = source_dataset.project
        self.client.update_dataset(source_dataset, ["access_entries"])
        logging.info(
            f"Access entries have been updated: {source_dataset.access_entries}"
        )

    def find_dataset(self, project_id, dataset_id):
        dataset_id_full = f"{project_id}.{dataset_id}"
        try:
            logging.info(f"Getting dataset {dataset_id_full}")
            self.client.project = project_id
            return self.client.get_dataset(dataset_id_full)
        except NotFound:
            raise Exception(f"Dataset {dataset_id_full} does not exist")

    def _get_view(self, dataset, view_id) -> bigquery.Table:
        try:
            self.client.project = dataset.project
            return self.client.get_table(dataset.table(view_id))
        except NotFound:
            logging.info(
                f"Table or view {view_id} is not found in dataset {dataset.dataset_id} ({dataset.project})"
            )
            return bigquery.Table(dataset.table(view_id))

    @staticmethod
    def _compose_view_query(
        source_project, source_dataset, source_table_id, custom_columns
    ):
        if custom_columns:
            return f"""
                      SELECT
                          {", ".join([f"`{col}`" for col in custom_columns])}
                      FROM
                          `{source_project}`.`{source_dataset.dataset_id}`.`{source_table_id}`
                  """
        return f"""
                  SELECT
                      *
                  FROM
                      `{source_project}`.`{source_dataset.dataset_id}`.`{source_table_id}`
              """

    def create_view(
        self,
        destination_dataset,
        source_dataset,
        source_table,
        source_table_columns_descriptions,
        custom_columns,
        view_name,
    ):
        self.client.project = destination_dataset.project

        view_ref = self._get_view(destination_dataset, view_name)

        if view_ref.created:
            self.client.delete_table(view_ref, not_found_ok=True)
            logging.info(f"Deleting view {view_ref.reference.to_api_repr()}")

        query = self._compose_view_query(
            source_dataset.project, source_dataset, source_table, custom_columns
        )
        view = bigquery.Table(view_ref)
        view.view_query = query

        logging.info(
            f"Creating view {view.reference.to_api_repr()} with query {' '.join(query.split())}"
        )
        created_view = self.client.create_table(view)
        logging.info(f"View {created_view.reference.to_api_repr()} has been created.")

        if source_table_columns_descriptions:
            # Apply column descriptions to the view
            self._update_columns_description(
                created_view, source_table_columns_descriptions
            )

        self._update_access(source_dataset, created_view)

    def delete_view(self, destination_dataset, view_name):
        view_ref = self._get_view(destination_dataset, view_name)
        if view_ref.created:
            self.client.delete_table(view_ref, not_found_ok=True)
            logging.info(f"Deleting view {view_ref.reference.to_api_repr()}")
        else:
            logging.info(f"View {view_ref.reference.to_api_repr()} does not exist.")

    def _update_columns_description(self, view, source_table_columns_descriptions):
        if not source_table_columns_descriptions:
            logging.info(
                f"No column descriptions to update for view {view.reference.to_api_repr()}"
            )
            return

        logging.info(
            f"Updating view {view.reference.to_api_repr()} with column descriptions."
        )
        new_schema = []

        for field in view.schema:
            description = None
            if field.name in source_table_columns_descriptions:
                description = source_table_columns_descriptions[field.name]
                logging.info(
                    f"Updating column {field.name} description to: {description}"
                )

            new_field = bigquery.SchemaField(
                name=field.name,
                field_type=field.field_type,
                mode=field.mode,
                description=description,
            )
            new_schema.append(new_field)

        view.schema = new_schema
        self.client.update_table(view, ["schema"])
        logging.info(
            f"View {view.reference.to_api_repr()} has been updated with column descriptions."
        )
