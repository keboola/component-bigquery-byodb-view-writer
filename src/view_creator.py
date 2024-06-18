import logging

from google.cloud import bigquery


class ViewCreator:
    def __init__(self, dst_client, src_client, source_project_id, source_dataset_id, source_table_id,
                 destination_project_id,
                 destination_dataset_id, destination_view_id,
                 custom_columns=None):
        self._src_client = src_client
        self._dst_client = dst_client
        self._source_dataset = self._src_client.get_dataset(source_project_id, source_dataset_id)
        self._source_table_id = source_table_id
        self._destination_view_id = destination_view_id
        self._destination_dataset = self._dst_client.get_dataset(destination_project_id, destination_dataset_id)
        self._custom_columns = custom_columns

    def _compose_view_query(self):
        if self._custom_columns:
            return f"""
                SELECT
                    {', '.join(self._custom_columns)}
                FROM
                    `{self._src_client.project}.{self._source_dataset.dataset_id}.{self._source_table_id}`
            """
        return f"""
            SELECT
                *
            FROM
                `{self._src_client.project}.{self._source_dataset.dataset_id}.{self._source_table_id}`
        """

    def _create_view(self):
        view = bigquery.Table(self._destination_dataset.table(self._destination_view_id))
        view.view_query = self._compose_view_query()
        view = self._dst_client.create_table(view)
        return view

    def _update_view_query(self, view):
        view.view_query = self._compose_view_query()
        view = self._dst_client.update_table(view, ["view_query"])
        return view

    def _update_access(self, view):
        access_entries = self._source_dataset.access_entries
        access_entry = bigquery.AccessEntry(None, "view", view.reference.to_api_repr())

        if access_entry not in access_entries:
            access_entries.append(access_entry)
            self._source_dataset.access_entries = access_entries
            self._dst_client.update_dataset(self._source_dataset, ["access_entries"])
            logging.info(f"View {view.reference.to_api_repr()} has been granted access to the source dataset.")
        else:
            logging.info(
                f"View {view.reference.to_api_repr()} already has access to the source dataset. ({access_entry})")

    def create_view(self):

        """
        Creates or updates a view in the destination dataset
        """
        existing_view = self._dst_client.get_view(self._destination_dataset.table(self._destination_view_id))

        if not existing_view:
            view = self._create_view()
            logging.info(f"View {view.reference.to_api_repr()} has been created.")
        else:
            view = self._update_view_query(existing_view)
            logging.info(f"View {view.reference.to_api_repr()} has been updated.")

        # self._update_access(view)
