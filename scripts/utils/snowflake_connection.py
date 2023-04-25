import os
import sqlalchemy as sql
from sqlalchemy.dialects import registry
from pathlib import Path
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import pd_writer, write_pandas

def query_warehouse(query, table):

    dotenv_path = Path('env/.env')
    load_dotenv(dotenv_path=dotenv_path)

    #get enviroment variables
    user_name = os.getenv('USER')
    user_password = os.getenv('USER_PASSWORD')
    wh_account_identifier = os.getenv('ACCOUNT_IDENTIFIER')

    #connect to snowflake and copy data from s3 bucket to snowflake
    snowflake_credentials = 'snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}'.format(
        user=user_name,
        password=user_password,
        account_identifier=wh_account_identifier,
        database_name ='IMDB',
        schema_name = 'PUBLIC'
    )

    aws_key = os.getenv('AWS_KEY')
    aws_secret = os.getenv('AWS_SECRET')

    registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')

    engine = sql.create_engine(snowflake_credentials)
    connect = engine.connect()

    connect.execute(f''' DELETE FROM {table}''')
    connect.execute(query)
        
    connect.close()
    engine.dispose()
        
