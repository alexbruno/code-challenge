from pandas import read_csv, read_sql, read_sql_table
from dagster import (
  asset,
  AssetExecutionContext,
  define_asset_job,
  Definitions,
  ScheduleDefinition
)
from functions import (
  create_db_result,
  db_connection,
  list_northwind_tables,
  order_details_file_path,
  psql_data_file_path,
  result_query_file_path,
)

# Data extraction assets

@asset
def extract_order_details(context: AssetExecutionContext):
  """
  Extracts order details from the source file and save them to a target file.
  """
  target = order_details_file_path()
  data = read_csv('data/order_details.csv')

  data.to_csv(target, index=False)

  context.add_output_metadata({
    'rows': len(data),
    'file': target,
  })

  return data

@asset
def extract_northwind_data(context: AssetExecutionContext):
  """
  Extracts data from the Northwind database tables and saves it to CSV files.
  """
  tables = list_northwind_tables()

  for table in tables:
    target = psql_data_file_path(table)
    data = read_sql_table(table, db_connection('northwind'))

    data.to_csv(target, index=False)

  context.add_output_metadata({
    'tables': len(tables),
  })

@asset(deps=[extract_order_details, extract_northwind_data])
def extract_data_assets():
  """
  Extracts data from the Northwind database tables and saves it to CSV files.
  """
  return None

# Data load assets

@asset(deps=[extract_data_assets])
def load_order_details():
  """
  Loads order details from the orders file to the result database.
  """
  target = order_details_file_path()
  data = read_csv(target)

  create_db_result()
  connect = db_connection('result')
  data.to_sql('order_details', connect, if_exists='replace', index=False)

  return data

@asset(deps=[extract_data_assets])
def load_northwind_data():
  """
  Loads data from the Northwind CSV files to the result database.
  """
  tables = list_northwind_tables()

  for table in tables:
    target = psql_data_file_path(table)
    data = read_csv(target)

    create_db_result()
    connect = db_connection('result')
    data.to_sql(table, connect, if_exists='replace', index=False)

  return tables

@asset(deps=[load_order_details, load_northwind_data])
def load_data_assets():
  """
  Loads data from the CSV files to the result database.
  """
  result_query = '''
  SELECT * FROM order_details
  JOIN orders ON order_details.order_id = orders.order_id
  JOIN products ON order_details.product_id = products.product_id;
  '''
  connect = db_connection('result')
  data = read_sql(result_query, connect)
  result_file = result_query_file_path()

  data.to_csv(result_file, index=False)

  return data

# Pipeline job definitions

pipeline_job = define_asset_job('pipeline_data_job')
pipeline_schedule = ScheduleDefinition(
  job=pipeline_job,
  cron_schedule='55 23 * * *',
)
pipeline_definitions = Definitions(
  assets=[
    extract_order_details,
    extract_northwind_data,
    extract_data_assets,
    load_order_details,
    load_northwind_data,
    load_data_assets
  ],
  jobs=[pipeline_job],
  schedules=[pipeline_schedule],
)
