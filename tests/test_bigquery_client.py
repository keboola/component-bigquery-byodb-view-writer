"""
Regression tests for the defensive retry added to
``BigqueryClient._update_access`` — it must retry on an HTTP 412
``PreconditionFailed`` (concurrent modification of the source dataset access
entries) with a freshly fetched dataset, and re-raise after the last attempt.
"""

import unittest
import mock

from google.api_core.exceptions import PreconditionFailed

from google_cloud.bigquery_client import BigqueryClient


def _make_view():
    view = mock.MagicMock()
    view.reference.project = "view-project"
    view.reference.dataset_id = "view_dataset"
    view.reference.table_id = "view_table"
    return view


def _make_dataset(access_entries=None):
    dataset = mock.MagicMock()
    dataset.project = "src-project"
    dataset.dataset_id = "src_dataset"
    dataset.access_entries = list(access_entries) if access_entries else []
    return dataset


def _client_without_credentials():
    # Bypass __init__ (which builds a real bigquery.Client) and inject a mock.
    client = BigqueryClient.__new__(BigqueryClient)
    client.client = mock.MagicMock()
    return client


class TestUpdateAccessRetry(unittest.TestCase):
    @mock.patch("google_cloud.bigquery_client.time.sleep", return_value=None)
    def test_happy_path_updates_once_without_refetch(self, _sleep):
        """A successful update must not retry or re-fetch (behaviour unchanged)."""
        client = _client_without_credentials()
        source_dataset = _make_dataset()
        view = _make_view()

        client._update_access(source_dataset, view)

        self.assertEqual(client.client.update_dataset.call_count, 1)
        self.assertEqual(client.client.get_dataset.call_count, 0)
        _sleep.assert_not_called()

    @mock.patch("google_cloud.bigquery_client.time.sleep", return_value=None)
    def test_retries_on_precondition_failed_then_succeeds(self, _sleep):
        """On a 412 the dataset is re-fetched and the update retried; a
        concurrently-added access entry on the fresh dataset is preserved."""
        client = _client_without_credentials()
        source_dataset = _make_dataset()
        view = _make_view()

        # A concurrent writer already added an unrelated access entry to the
        # dataset by the time we re-fetch it.
        concurrent_entry = mock.MagicMock()
        concurrent_entry.entity_type = "user"
        refetched = _make_dataset(access_entries=[concurrent_entry])
        client.client.get_dataset.return_value = refetched

        # First update hits the ETag conflict, the retry succeeds.
        client.client.update_dataset.side_effect = [PreconditionFailed("412"), None]

        client._update_access(source_dataset, view)

        self.assertEqual(client.client.update_dataset.call_count, 2)
        client.client.get_dataset.assert_called_once_with("src-project.src_dataset")
        # The concurrent entry survived and our view was appended on top of it.
        self.assertIn(concurrent_entry, refetched.access_entries)
        self.assertEqual(len(refetched.access_entries), 2)

    @mock.patch("google_cloud.bigquery_client.time.sleep", return_value=None)
    def test_reraises_after_persistent_precondition_failed(self, _sleep):
        """A persistent 412 must still fail loudly after the last attempt."""
        client = _client_without_credentials()
        view = _make_view()
        client.client.get_dataset.return_value = _make_dataset()
        client.client.update_dataset.side_effect = PreconditionFailed("412")

        with self.assertRaises(PreconditionFailed):
            client._update_access(_make_dataset(), view)

        # 5 attempts total, re-fetching before each of the 4 retries.
        self.assertEqual(client.client.update_dataset.call_count, 5)
        self.assertEqual(client.client.get_dataset.call_count, 4)


if __name__ == "__main__":
    unittest.main()
