# -*- coding: utf-8 -*-
import os
import unittest

from file_parser import CollectData, ImportData, DataMapping, SQL_DATA_TEXT, SQL_DATA_BOOLEAN, SQL_DATA_INTEGER
from settings import DATA_PATH, DIRECTORY_BASE_PATH, PARSED_DATA_PATH


class CollectDataTests(unittest.TestCase):

    def test_creates_instances_of_ImportData(self):
        files = CollectData()
        self.assertIsInstance(files.file_pairs[0], ImportData)

    def test_finds_files(self):
        files = CollectData()
        self.assertIn("testformat1_2016-08-22.txt", files.file_pairs[0].data_files[0])


class ImportDataTests(unittest.TestCase):

    def setUp(self):
        self.files = CollectData()
        self.import_instance = self.files.file_pairs[0]
        self.parsed_data_path = DIRECTORY_BASE_PATH + PARSED_DATA_PATH
        self.data_path = DIRECTORY_BASE_PATH + DATA_PATH

    def tearDown(self):
        files = ["{0}{1}".format(self.parsed_data_path, f) for f in os.listdir(self.parsed_data_path) if os.path.isfile(os.path.join(self.parsed_data_path, f))]
        for _file in files:
            file_name = _file.split("/")[-1]
            new_destination = "{0}{1}".format(self.data_path, file_name)
            os.rename(_file, new_destination)

    def test_get_table_name(self):
        self.import_instance._get_table_name()
        self.assertEqual(self.import_instance.table_name, "testformat1")

    def test_get_columns(self):
        self.import_instance._get_columns()
        self.assertEqual(self.import_instance.columns[0].column_name, "name")
        self.assertEqual(self.import_instance.column_names, ["name", "valid", "price"])
        self.assertEqual(self.import_instance.widths, [10, 1, 3])

    def test_parse_data(self):
        self.import_instance._get_columns()
        self.import_instance._parse_data()
        self.assertEqual(len(self.import_instance.data), 3)
        self.assertEqual(self.import_instance.data[0][0].value, "Foonyor")
        self.assertEqual(self.import_instance.data[0][2].datatype, "INTEGER")

    def test_cast_data(self):
        data = DataMapping("Foonyor", SQL_DATA_TEXT)
        formated_data = self.import_instance._cast_data(data)
        self.assertEqual(formated_data, "'Foonyor'")

        data2 = DataMapping("0", SQL_DATA_BOOLEAN)
        formated_data2 = self.import_instance._cast_data(data2)
        self.assertEqual(formated_data2, False)

        # when string formatting the "10" becomes 10 in the SQL command
        data3 = DataMapping("10", SQL_DATA_INTEGER)
        formated_data3 = self.import_instance._cast_data(data3)
        self.assertEqual(formated_data3, "10")

    def test_move_data_files(self):
        self.import_instance._move_data_files()
        directory_to_search = DIRECTORY_BASE_PATH + DATA_PATH
        files = len([f for f in os.listdir(directory_to_search) if os.path.isfile(os.path.join(directory_to_search, f))])
        self.assertEqual(files, 0)


if __name__ == '__main__':
    unittest.main()