from os import environ, makedirs
from datetime import datetime
from pandas import read_sql
from psycopg2 import connect

def db_connection(db):
  """
  Function to connect to the PostgreSQL database.
  """
  return f'postgresql://northwind_user:thewindisblowing@localhost:5432/{db}'

def today():
  """
  Return the current date in the format "YYYY-MM-DD".
  """
  now = datetime.now()
  return now.__format__("%Y-%m-%d")

def order_details_file_path():
  """
  Function to retrieve the order details target file path.
  """
  date = environ.get('DATE', today())
  dir = f'data/csv/{date}'
  file = f'{dir}/order_details.csv'

  makedirs(dir, exist_ok=True)

  return file

def psql_data_file_path(table):
  """
  Function to retrieve the target data file path for the given table in the PostgreSQL database.

  Parameters:
  table (str): The name of the table in the PostgreSQL database.

  Returns:
  str: The file path for today's data in the specified table.
  """
  date = environ.get('DATE', today())
  dir = f'data/postgres/{table}/{date}'
  file = f'{dir}/data.csv'

  makedirs(dir, exist_ok=True)

  return file

def result_query_file_path():
  """
  Function to retrieve the result query target file path.
  """
  date = environ.get('DATE', today())
  dir = f'data/result/{date}'
  file = f'{dir}/result.csv'

  makedirs(dir, exist_ok=True)

  return file

def list_northwind_tables():
  """
  Retrieve a list of table names from the Northwind database.
  """
  tables = read_sql(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    db_connection('northwind')
  )
  return tables['table_name'].to_list()

def create_db_result():
  """
  Function to create a database named 'result' if it does not already exist.
  """
  db = db_connection('northwind')
  conn = connect(db)
  conn.autocommit = True

  with conn.cursor() as cursor:
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'result'")

    if not cursor.fetchone():
      cursor.execute("CREATE DATABASE result")

    cursor.close()
  conn.close()
