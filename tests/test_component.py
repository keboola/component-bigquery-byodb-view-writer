"""
Created on 12. 11. 2018

@author: esner
"""

import unittest
import mock
import os
from freezegun import freeze_time

from component import Component


class TestComponent(unittest.TestCase):
    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


class TestNativeDescriptions(unittest.TestCase):
    TABLE = {
        "definition": {
            "description": "Native table text",
            "columns": [
                {"name": "id", "definition": {"type": "INTEGER", "description": "Native column text"}},
                {"name": "note", "definition": {}},
                {"name": "amount", "definition": None},
            ],
        },
        # legacy metadata must not be read
        "metadata": [{"key": "KBC.description", "value": "stale table text"}],
        "columnMetadata": {"id": [{"key": "KBC.description", "value": "stale column text"}]},
    }

    def test_table_description_is_read_from_definition(self):
        self.assertEqual("Native table text", Component._get_table_description(self.TABLE))

    def test_fields_descriptions_are_read_from_definition_columns(self):
        self.assertEqual({"id": "Native column text"}, Component._get_fields_descriptions(self.TABLE))

    def test_missing_definition_gives_no_descriptions(self):
        table = {"metadata": self.TABLE["metadata"], "columnMetadata": self.TABLE["columnMetadata"]}
        self.assertIsNone(Component._get_table_description(table))
        self.assertEqual({}, Component._get_fields_descriptions(table))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
