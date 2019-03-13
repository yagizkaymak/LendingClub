#!/usr/bin/env python3

""" This class performs data cleaning and validation """

import numpy as np
import pandas as pd
import os
import re

# Import user-defined libraries
from logger.logger import get_logger

class ETL:

    def __init__(self, logger):
        self.logger = logger


    def loan_condition(self, emp_length):
        """ Helper method for data cleaning, which replaces employement length with integers

        Parameters
        ----------
        emp_length (Type: str): Employement length variable to be converted to integer

        Returns
        -------
        Corresonsing int value for "emp_length" variable
        """
        if emp_length.strip() == '10+':
            return 10

        if emp_length.strip() == '< 1 year':
            return 0

    def check_if_numeric(self, df):
        """ Method that replaces all non-numeric rows that are not nullable with zeros

        Parameters
        ----------
        df (pandas dataframe): Input dataframe to be checked

        Returns
        -------
        df (pandas dataframe): Dataframe having zeros for not nullable columns
        """
        # Replace all non-numeric rows that are not nullable with zeros
        numeric_cols = ['loan_amnt', 'funded_amnt', 'funded_amnt_inv', 'int_rate', 'installment',
                        'emp_length', 'annual_inc', 'dti', 'delinq_2yrs', 'inq_last_6mths', 'mths_since_last_delinq',
                        'mths_since_last_record', 'open_acc', 'pub_rec', 'revol_bal', 'revol_util', 'total_acc',
                        'out_prncp', 'out_prncp_inv', 'total_pymnt', 'total_pymnt_inv', 'total_rec_prncp',
                        'total_rec_int', 'total_rec_late_fee', 'recoveries', 'collection_recovery_fee',
                        'last_pymnt_amnt', 'policy_code'
                        ]

        for col in numeric_cols:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            except (Exception) as e:
                self.logger.error("Numeric conversion failed! " + str(e))

        return df


    def convert_to_date(self, df):
        """ Method that converts all text-based data points to datetime objects

        Parameters
        ----------
        df (pandas dataframe): Input dataframe to be converted

        Returns
        -------
        df (pandas dataframe): Dataframe with converted datetime values
        """

        # Convert date columns to dates
        date_cols = ['issue_d', 'earliest_cr_line', 'last_credit_pull_d', 'last_pymnt_d', 'next_pymnt_d']

        for col in date_cols:
            try:
                df[col] = pd.to_datetime(df[col], format='%b-%Y').dt.date
            except (Exception) as e:
                self.logger.error("Date conversion failed! " + str(e))

        return df

    def convert_to_positive(self, df):
        """ Method that converts all negative numeric data points to positive if there is any

        Parameters
        ----------
        df (pandas dataframe): Input dataframe to be converted

        Returns
        -------
        df (pandas dataframe): Dataframe with numeric values converted to positive
        """

        # Get only the numeric variables and make sure they are all positive
        numeric_columns = df.select_dtypes(exclude=["object"])
        for col in numeric_columns:
            df[col] = df[col].apply(lambda x: abs(x))

        return df


    def miscellaneous(self, df):
        """ Method that performs miscellaneous data cleaning and validation for some columns of the given dataframe

        Parameters
        ----------
        df (pandas dataframe): Input dataframe to be cleaned and validated

        Returns
        -------
        df (pandas dataframe): Dataframe with cleaned and validated values
        """

        # Replace if there is any n/a field in the dataframe
        df.replace('n/a', np.nan, inplace=True)

        # Remove all duplicate rows from the given dataframe
        df = df.drop_duplicates()

        # Replacing "employment length" column with integer number as follows:
        # Replace by zero if it is NULL
        # Replace "10+" with "10" and
        # Replace "< 1" with "0" as described in the loan stats dictionary
        # Remove all non-numeric chars
        df['emp_length'].fillna(value=0, inplace=True)
        df['emp_length'].replace(to_replace='10+ years', value='10', inplace=True)
        df['emp_length'].replace(to_replace='< 1 year', value='0', inplace=True)
        df['emp_length'].replace(to_replace='[^0-9]+', value='', inplace=True, regex=True)

        # Remove leading whitespaces and get the integer part of the "term" column
        df['term'] = df['term'].apply(lambda x: x.strip())
        df['term'] = df['term'].apply(lambda x: int(x.split()[0]))

        # Annual income can not be NULL. Replace those rows with zeros.
        df['annual_inc'].fillna(value=0, inplace=True)

        # Replace NULL values in "verification_status" with 'Not Verified'
        df['verification_status'].fillna(value='Not Verified', inplace=True)

        # FUTURE WORK: There are some rows with debt-to-income (dti) ratio equal to zero
        # In order to give a better estimation about dti
        # an estimate dti can be calculated by calculating
        # installment/(annual_inc/12)*100


        # There is an ID as "Loans that do not meet the credit policy".
        # There could me more. Remove them by dropping rows with no
        # member id.
        indexNames = df[pd.isna(df['member_id'])].index

        # Delete these row indexes from dataFrame
        df.drop(indexNames , inplace=True)

        # delinq_2yrs can not be NULL. Replace NULL values with zeros.
        df['delinq_2yrs'].fillna(value=0, inplace=True)

        # inq_last_6mths can not be NULL. Replace NULL values with zeros.
        df['inq_last_6mths'].fillna(value=0, inplace=True)

        return df


    def clean_and_validate(self, df):
        """
        Method that cleans and validates the contents of the dataframe provided as the input

        Parameters
        ----------
        df (Type: pandas.DataFrame): Dataframe with values to be cleaned and validated

        Returns
        -------
        df (Type: pandas.DataFrame): Dataframe with cleaned and validated values
        """

        # Do numeric control on the dataset
        df = self.check_if_numeric(df)

        # Perform a conversion to make sure that there is no negative numeric values
        df = self.convert_to_positive(df)

        # Perform date conversion for text-based dates
        df = self.convert_to_date(df)

        # Perform extra data cleaning and validation
        df = self.miscellaneous(df)

        return df
