#!/usr/bin/env python3
""" This class contains the configuration for database and logs """

import os


DEFAULT_LOGGING_LEVEL = "INFO"
DEFAULT_LOGGING_DIR =  str(os.path.expanduser("~/LendingClub")) + "/logs/"
DEFAULT_LOGGING_FILE_NAME = "lc.log"
DEFAULT_LOGS_ROTATE_WHEN = "midnight"
DEFAULT_LOGS_ROTATE_BACKUP_COUNT = 7 #
DB_USER = '' # Database username
DB_PASS = '' # Database password
DB_HOSTNAME = '' # Database hostname
DB_PORT = '' # Database port number
DB_TABLE_NAME = 'loan' # Table name in the database
INSERTION_CHUNKSIZE = 1000 # Number of rows will be written in batches of this size at a time.
INSERTION_METHOD = 'multi' # Controls the SQL insertion clause used: ‘multi’: Pass multiple values in a single INSERT clause.


class Configuration:

    def __init__(self):
        """ Initializes the Configuration class and fetches DB credentials from environment variables

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        self.DB_USER = os.getenv('POSTGRES_USER')
        self.DB_PASS = os.getenv('POSTGRES_PASS')
        self.DB_HOSTNAME = os.getenv('POSTGRES_HOSTNAME')
        self.DB_PORT = os.getenv('POSTGRES_PORT')


    def get_logging_level(self):
        """ Returns logging level for logger object

        Parameters
        ----------
        None

        Returns
        -------
        DEFAULT_LOGGING_LEVEL (str): Default logging level

        """
        return DEFAULT_LOGGING_LEVEL


    def get_logs_output_file_path(self):
        """ Returns logs output file with the combination of folder name

        Parameters
        ----------
        None

        Returns
        -------
        logging_dir + logging_file_name (str): Combination of log folder name and log file name

        """
        logging_dir = DEFAULT_LOGGING_DIR
        logging_file_name = DEFAULT_LOGGING_FILE_NAME
        if logging_dir is not None and logging_file_name is not None:
            return logging_dir + logging_file_name
        else:
            return None

    def get_logs_rotate_when(self):
        """ Returns a string indicating when log rotation happens

        Parameters
        ----------
        None

        Returns
        -------
        DEFAULT_LOGS_ROTATE_WHEN (str): String log rotation time

        """
        return DEFAULT_LOGS_ROTATE_WHEN

    def get_logs_rotate_backup_count(self):
        """ Returns the number of log rotation period in days

        Parameters
        ----------
        None

        Returns
        -------
        DEFAULT_LOGS_ROTATE_WHEN (int): Log rotation period in days

        """
        return DEFAULT_LOGS_ROTATE_BACKUP_COUNT

    def get_db_table_name(self):
        """ Returns table name to insert in the database

        Parameters
        ----------
        None

        Returns
        -------
        DB_TABLE_NAME (str): Table name to insert
        """
        return DB_TABLE_NAME

    def get_insertion_chunksize(self):
        """ Returns the number of rows that will be written in batches of this size at a time.

        Parameters
        ----------
        None

        Returns
        -------
        INSERTION_CHUNKSIZE (int): Number of rows to insert at a time
        """
        return INSERTION_CHUNKSIZE

    def get_insertion_method(self):
        """ Returns the insertion method to db.

        Parameters
        ----------
        None

        Returns
        -------
        INSERTION_METHOD (str): Returns the insertion method to database
        """
        return INSERTION_METHOD

    def get_db_uri(self):
        """ Constructs and returns the connection string for database

        Parameters
        ----------
        None

        Returns
        -------
        conn_string (str): Connection string for database
        """
        conn_string = f'postgresql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOSTNAME}:{self.DB_PORT}/lending_club'
        return conn_string
