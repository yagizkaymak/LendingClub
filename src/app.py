""" This script is the main entry point of extract-transform-load (ETL) process for Lending Club Loans dataset

Author: Yagiz Kaymak
Date: 3/13/2019

"""
import os
import sys
import logging
import numpy as np
import pandas as pd


# Import user libraries
from configuration import Configuration
from db.sql_metadata_service import SQLMetadataService
from logger.logger import get_logger
from etl.ETL import ETL

# Get the configuration object to access config variables
configuration = Configuration()

# Create logger object with given configuration
logger = get_logger(
    logging_level=configuration.get_logging_level(),
    logs_output_file_path=configuration.get_logs_output_file_path(),
    logs_rotate_when=configuration.get_logs_rotate_when(),
    logs_rotate_backup_count=configuration.get_logs_rotate_backup_count()
)

def main(argv):
    """ Main function and the entry point of the lending club loan ETL application.

    Parameters
    ----------
    argv (Type: str list): Command line arguments

    Returns
    ----------
    None
    """

    logger.info("Lending Club ETL application has started!")
    print("Lending Club ETL application has started!")

    # Second command line argument is the input file (csv file)
    if len(argv) < 2:
        logger.error("Missing input csv file! Exiting...")
        print("Missing input csv file! Exiting...")
        sys.exit()

    # Get csv file name
    csv_filename = argv[1]


    # Load csv file as a pandas dataframe
    logger.info("Csv file is being loaded to a pandas dataframe")
    print("Csv file is being loaded to a pandas dataframe")
    loan_df = pd.read_csv(csv_filename, low_memory=False)
    logger.info(str(argv[1]) + " has been loaded into dataframe")
    print("Csv file has been loaded into dataframe")

    # In case we need the original data create a copy
    original_df = loan_df.copy()

    # Instantiate an etl object to be used for data cleaning and validation
    etl = ETL(logger)
    logger.info("Imported dataframe is being cleaned and validated...")
    print("Imported dataframe is being cleaned and validated...")
    loan_df = etl.clean_and_validate(loan_df)
    logger.info("Imported dataframe has been cleaned and validated.")
    print("Imported dataframe has been cleaned and validated")

    # Get the db configuration
    chunksize = configuration.get_insertion_chunksize()
    method = configuration.get_insertion_method()
    table_name = configuration.get_db_table_name()
    sql_alchemy_conn = configuration.get_db_uri()

    # Instantiate SQLMetadataService.
    metadata_service = SQLMetadataService(sql_alchemy_conn,logger)

    # Initialize SQLMetadataService object which creates the loan table in the DB
    metadata_service.initialize_metadata_source()


    # Insert cleaned and validated data to DB
    print("Data is being inserted to DB...")
    try:
        metadata_service.insert_into_db(loan_df, chunksize, table_name, method)
        print("Insertion to DB is completed")

    except (Exception) as e:
            print("DB insertion failed! " + str(e))


if __name__=="__main__":
    main(sys.argv)
