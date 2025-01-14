import snowflake.connector as sc
import logging
import sys
import os
from spire.xls import *
from spire.xls.common import *

#os.environ["username"] =
#os.environ["password"] =
#os.environ["account"] =

raw_data = [
            {'table_name': 'device', 'columns': 'id number, type number, store_id number'},
            {'table_name': 'store', 'columns': 'id number, name varchar(200), address varchar(200), city varchar(200), country varchar(200), created_at TIMESTAMP_NTZ, typology varchar(200),customer_id number'},
            {'table_name': 'transaction', 'columns': 'id number, device_id number, product_name varchar(200), product_sku varchar(200), category_name varchar(200), amount number, status varchar(200), card_number varchar(200), cvv number, created_at TIMESTAMP_NTZ, happened_at TIMESTAMP_NTZ'}
            ]

dbt_models = [
          'staging',
          'dimensions',
          'bridges',
          'facts',
          'datamarts'
          ]

vars = {"execution_ts": "2022-10-01"}

def convert_to_csv(raw_data):
    for i in raw_data:
        workbook = Workbook()
        workbook.LoadFromFile(f"{i['table_name']}.xlsx")
        sheet = workbook.Worksheets[0]
        sheet.SaveToFile(f"{i['table_name']}.csv", "|||", Encoding.get_UTF8())
        workbook.Dispose()

def connect_snowflake():
    ctx = sc.connect(
                    user=os.environ["username"],
                    password=os.environ["password"],
                    account=os.environ["account"],
                    warehouse="COMPUTE_WH",
                    database="SUMUP_TAKEHOMETASK",
                    schema="RAW",
                    internal_application_version="2.7.8"
    )
    cs = ctx.cursor()
    return(cs)

def raw_data_to_snowflake(raw_data,cs):
    logging.info('Schemas created')
    for i in raw_data:
        cs.execute(f"CREATE OR REPLACE TABLE {i['table_name']}({i['columns']})")
        cs.execute(f"PUT file://{i['table_name']}.csv @%{i['table_name']} OVERWRITE=TRUE")
        cs.execute(f"COPY INTO {i['table_name']} FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_DELIMITER='|||')")
        os.remove(f"{i['table_name']}.csv")

def dbt_run(dbt_models, vars):
    for i in dbt_models[:3]:
        os.system(f"dbt run --full-refresh --vars '{vars}' --models {i}")
    os.system("dbt snapshot")
    for i in dbt_models[3:]:
        os.system(f"dbt run --full-refresh --vars '{vars}' --models {i}")



def main():
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO, stream=sys.stdout)
    logging.info('Converting xlsx to CSV started.')
    convert_to_csv(raw_data)
    logging.info('Conversion to CSV successful.')
    logging.info('Snowflake connection started.')
    cs = connect_snowflake()
    logging.info('Snowflake connection successful.')
    logging.info('Data load to snowflake started.')
    raw_data_to_snowflake(raw_data,cs)
    logging.info('Data load to snowflake successful.')
    logging.info('Dbt run models started')
    dbt_run(dbt_models, vars)
    logging.info('Dbt models created successfully')


if __name__ == '__main__':
    main()

