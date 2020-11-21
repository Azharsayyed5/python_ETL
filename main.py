import os
from environs import Env
from sqlalchemy import create_engine, MetaData, Column, Integer, String, Table, DATE, VARCHAR, CHAR
from sqlalchemy.orm.session import sessionmaker
import pandas as pd

# File location at current path, same path as of python script
file_path = f'{os.path.dirname(os.path.abspath(__file__))}/customer_sheet.csv'

# Database credentials
env = Env()
env.read_env()
db_username = env("DB_USERNAME")
db_password = env("DB_PASSWORD")
db_name = env("DB_NAME")
db_host = env("DB_HOST")
db_port = env.int("DB_PORT")


def create_table(table_name, country_name=''):
    try:
        # Create table for table name passed in arguments
        table_name = table_name if table_name == 'staging' else f'Table_{country_name}'
        meta = MetaData()
        customers = Table(table_name, meta,
                          Column('name', VARCHAR(255), nullable=False),
                          Column('customer_id', VARCHAR(18), primary_key=True, nullable=False),
                          Column('open_date', DATE, nullable=False),
                          Column('consulted_date', DATE, nullable=True),
                          Column('vac_id', CHAR(5), nullable=True),
                          Column('dr_name', CHAR(255), nullable=True),
                          Column('state', CHAR(5), nullable=True),
                          Column('country', CHAR(5), nullable=True),
                          Column('dob', DATE, nullable=True),
                          Column('is_active', CHAR(1), nullable=True)
                         )
        meta.create_all(sqlEngine)
        return customers
    except Exception as TableCreationError:
        print(TableCreationError)
        return False


def insert_into_table(dataframe, table_obj):
    try:
        # Insert Bulk records in particular table
        buffer = []
        for index, row in dataframe.iterrows():
            buffer.append(
                dict(
                    name=row['customer_name'],
                    customer_id=row['customer_id'],
                    open_date=row['open_date'],
                    consulted_date=row['last_consulted_date'],
                    vac_id=row['vaccination_id'],
                    dr_name=row['dr_name'],
                    state=row['state'],
                    country=row['country'],
                    dob=row['dob'],
                    is_active=row['is_active']
                )
            )

        dbConnection.execute(table_obj.insert(), buffer)
    except Exception as InsertionError:
        print(InsertionError)
        return False


def process_country_wise(dataframe):
    try:
        for country, country_records in dataframe.groupby(['country']):
            # create country table
            table_obj = create_table('non_staging', country)
            # Insert per country records
            insert_into_table(country_records, table_obj)
    except Exception as GeneralException:
        print(GeneralException)
        return False


if __name__ == '__main__':
    # MySQL Connection
    sqlEngine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}/{db_name}', echo=True)
    dbConnection = sqlEngine.connect()

    # Read data from file
    df = pd.read_csv(file_path, usecols=['customer_name', 'customer_id', 'open_date', 'last_consulted_date', 'vaccination_id', 'dr_name', 'state', 'country', 'dob', 'is_active'])
    # Create staging table
    table_object = create_table('staging')
    # Insert staging records
    insert_into_table(df, table_object)
    # Process country wise data
    process_country_wise(df)

