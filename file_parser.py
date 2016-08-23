# -*- coding: utf-8 -*-
import psycopg2
from os import listdir, rename
from os.path import isfile, join
from collections import namedtuple

from database import get_connection
from settings import DIRECTORY_BASE_PATH, DATA_PATH, SPECS_PATH, PARSED_DATA_PATH

SQL_DATA_TEXT = "TEXT"
SQL_DATA_BOOLEAN = "BOOLEAN"
SQL_DATA_INTEGER = "INTEGER"

CONNECTION = get_connection()
CURSOR = CONNECTION.cursor()

ColumnMapping = namedtuple("ColumnMapping", "column_name width datatype")
DataMapping = namedtuple("DataMapping", "value datatype")


class ImportData:
    """
    helper class that takes in spec and data files
    ImportData.import_data() is main executable:
         - table is created if needed
         - data and columns are parsed
         - data is inserted into the DB
         - data files are moved to ./parsed_data/
    """
    SPEC_FILE = "spec_file"
    DATA_FILES = "data_files"

    def __init__(self, file_dict):
        try:
            self.spec_file = file_dict[self.SPEC_FILE]
            self.data_files = file_dict[self.DATA_FILES]
        except KeyError:
            raise ValueError("Missing data or spec files")

        self.table_name = None
        self.data = []
        self.columns = []
        self.created_table = False
        self.table_command = None
        self.widths = []
        self.column_names = []

    def import_data(self):
        self._get_table_name()
        self._get_columns()
        self._get_or_create_table()
        self._parse_data()
        self._import_data()
        self._move_data_files()

    def execute_command(self, command):
        """
        used to run SQL commands and catch any execution errors
        :param command: SQL command to run
        :return: boolean
        """
        try:
            CURSOR.execute(command)
            CONNECTION.commit()
            return True
        except psycopg2.Error as e:
            return False

    def _get_table_name(self):
        self.table_name = self.spec_file.split('/')[-1].split('.')[0]

    def _get_columns(self):
        """
        reads the spec file to understand the schema
        :return: gets column names, columns in the ColumnMapping format, and the widths
        """
        with open(self.spec_file) as f:
            content = f.readlines()[1:]
            for index, line in enumerate(content):
                line_values = line.strip().split(',')
                column_mapping = ColumnMapping(line_values[0], line_values[1], line_values[2])
                self.columns.append(column_mapping)
                self.column_names.append(column_mapping.column_name)
                self.widths.append(int(column_mapping.width))

        f.close()

    def _get_or_create_table(self):
        """
        check if table exists and if not creates one
        :return: if no table, updates table_command and created_table
        """
        search_query = "SELECT relname FROM pg_class WHERE relname = '{0}'".format(self.table_name)
        CURSOR.execute(search_query)
        exists = CURSOR.fetchone()
        if not exists:
            command = "CREATE TABLE {0} (id SERIAL PRIMARY KEY, ".format(self.table_name)
            columns_with_datatype = ["{0} {1}".format(c.column_name, c.datatype) for c in self.columns]
            command += "{0});".format(", ".join(columns_with_datatype))
            self.table_command = command
            self.execute_command(command)
            self.created_table = True

    def _parse_data(self):
        """
        reads each data file and maps the data to DataMapping
        TODO: update so can work with more than 3 columns
        :return: populates self.data with 3 DataMapping instances
        """
        for _file in self.data_files:
            with open(_file) as f:
                content = f.readlines()
                for line in content:
                    line_values = line.strip()
                    column1_data = line_values[:(self.widths[0]-1)].strip()
                    column2_data = line_values[(self.widths[0]-1):(self.widths[1] + self.widths[0])].strip()
                    column3_data = line_values[(self.widths[0] + self.widths[1]):(self.widths[0] + self.widths[1] + self.widths[2] + 1)].strip()
                    data_mapping_column1 = DataMapping(column1_data, self.columns[0].datatype)
                    data_mapping_column2 = DataMapping(column2_data, self.columns[1].datatype)
                    data_mapping_column3 = DataMapping(column3_data, self.columns[2].datatype)
                    self.data.append([data_mapping_column1, data_mapping_column2, data_mapping_column3])

            f.close()

    def _cast_data(self, data):
        """
        :param data: DataMapping instance
        :return: value cast for the SQL command
        """
        value = data.value
        data_type = data.datatype
        if data_type == SQL_DATA_BOOLEAN:
            return bool(int(value))

        if data_type == SQL_DATA_TEXT:
            return "'{0}'".format(value)

        return value

    def _import_data(self):
        """
        Actually imports the data once it has been processed
        TODO: update to work with more than 3 columns
        :return:
        """
        for data in self.data:
            command = "INSERT INTO {0} ({1}) VALUES (".format(self.table_name, ", ".join(self.column_names))
            column1_data = self._cast_data(data[0])
            column2_data = self._cast_data(data[1])
            column3_data = self._cast_data(data[2])
            command += "{0}, {1}, {2});".format(column1_data, column2_data, column3_data)
            self.execute_command(command)

    def _move_data_files(self):
        """
        move all data files once parsed into the ./parsed_data/ folder
        :return:
        """
        for _file in self.data_files:
            file_name = _file.split("/")[-1]
            new_destination = "{0}{1}{2}".format(DIRECTORY_BASE_PATH, PARSED_DATA_PATH, file_name)
            rename(_file, new_destination)


class CollectData:

    def __init__(self):
        self.file_pairs = list()
        self.organize_files()

    def get_files(self, path):
        path_to_check = "{0}{1}".format(DIRECTORY_BASE_PATH, path)
        return ["{0}{1}".format(path_to_check, _file) for _file in listdir(path_to_check) if
                isfile(join(path_to_check, _file))]

    def organize_files(self):
        """
        sort files by specification and create ImportData instances
        """
        specs_files = self.get_files(SPECS_PATH)
        if not len(specs_files):
            return

        data_files = self.get_files(DATA_PATH)
        for _file in specs_files:
            file_dict = {
                ImportData.SPEC_FILE: _file,
                ImportData.DATA_FILES: []
            }
            file_name = _file.split('/')[-1].split('.')[0]
            for data_file in data_files:
                # TODO: could be looking at the same files 2x 3x etc, devise better solution
                if file_name in data_file:
                    file_dict[ImportData.DATA_FILES].append(data_file)

            self.file_pairs.append(ImportData(file_dict))


def import_data():
    """
    Run this to parse and import new data / tables
    :return:
    """
    data = CollectData()
    if len(data.file_pairs):
        for import_data_instances in data.file_pairs:
            import_data_instances.import_data()
            table_name = import_data_instances.table_name
            print "Import {0} items into {1}".format(len(import_data_instances.data), table_name)
            count_command = "SELECT COUNT(*) from {0};".format(table_name)
            CURSOR.execute(count_command)
            count = CURSOR.fetchone()
            print "Table {0} has {1} items now".format(table_name, int(count[0]))

    CURSOR.close()
    CONNECTION.close()
