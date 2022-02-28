# etl.py

import argparse
import pandas as pd
import logging
from initialize_database import connect_to_db, create_customer_table

CUSTOMER_TABLE_NAME = 'CUSTOMER_INFO'
DATABASE_NAME = 'CUSTOMER'

def setup_db():
    """ Sets up a database and table or connects to the existing one

    Returns:
        connection(sqlite3.connection) - connection obj to database
    """
    connection = connect_to_db(DATABASE_NAME+'.db')
    create_customer_table(connection, CUSTOMER_TABLE_NAME)
    return connection

def transform(input_df):
    """ Transforms input data frame:
        1. Matches the database column name
        2. checks for missing values and removes the row
        3. checks for invalid ip address and removes the row

    Arguments:
        input_df(pd.DataFrame)

    Returns:
        transformed_df(pd.DataFrame)

    """

    logging.info("Transforming..")
    # modify the dataframe col names to match table col names
    logging.info("Renamed column id to customer_id")
    transformed_df = input_df.rename(columns=({ 'id': 'customer_id'}))
    # check for na and remove any
    logging.info("removed any rows with na")
    transformed_df.dropna(inplace=True)

    # check if ip address format is incorrect
    logging.info("removed rows with invalid ip")
    transformed_df['valid_ip'] = transformed_df.ip_address.str.match(
                            '^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$')
    transformed_df = transformed_df[transformed_df['valid_ip']].drop(
                            columns=['valid_ip'])

    return transformed_df

def load(connection, transformed_df):
    """ Loads pandas dataframe into database

    Arguments:
        connection(sqlite3.connection)
        transformed_df(pd.DataFrame)
    """
    transformed_df.to_sql(CUSTOMER_TABLE_NAME, connection,
                          if_exists='append', index = False)


def main():
    """ Extracts, Transforms and Loads datafile into sqlite database
    """
    logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_file', type=str,
                        help='data file name, with path if not '+
                        'in current folder',
                        required=True)
    args = parser.parse_args()

    # setup or connect to database
    logging.info("Setting up or connecting to Database")
    connection = setup_db()

    # Extract
    input_df = pd.read_csv(args.data_file)
    logging.info(f"ingested data from {args.data_file}")

    # Transform
    transformed_df = transform(input_df)
    logging.info("Transformed input data")
    # Load
    load(connection, transformed_df)
    logging.info(f"Loaded transformed data to {CUSTOMER_TABLE_NAME} table")


    connection.close()

if __name__ == '__main__':
    main()
