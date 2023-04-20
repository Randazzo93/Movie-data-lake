import requests
import movie_id_scrape as movies
import os
from dotenv import load_dotenv
import json
import boto3
import pandas as pd
import sqlalchemy as sql
from snowflake.connector.pandas_tools import pd_writer, write_pandas
from datetime import datetime
from pathlib import Path

dotenv_path = Path('env/.env')
load_dotenv(dotenv_path=dotenv_path)

url = "https://moviesdatabase.p.rapidapi.com/titles/x/titles-by-ids"

#get enviroment variables
secret_key = os.getenv('SECRET_KEY')
aws_key = os.getenv('AWS_KEY')
aws_secret = os.getenv('AWS_SECRET')
user_name = os.getenv('USER')
user_password = os.getenv('USER_PASSWORD')
wh_account_identifier = os.getenv('ACCOUNT_IDENTIFIER')

#get movie id from IMBD
movie_id_list = movies.get_movie_id()

request_back = 0
runs = 0

#Make request to moviedb API (Most request that can be made at 1 time is 25)
while len(movie_id_list)/25 > runs:
    
	runs += 1

	if(request_back == 0):
		movie_ids = (','.join(movie_id_list[0:24]))
	else:
		end_list = request_back + 24
		movie_ids = (','.join(movie_id_list[request_back:end_list]))
	query_movie_ids = '"' + movie_ids + '"'

	query = {"idsList":query_movie_ids}

	headers = {
		"X-RapidAPI-Key": secret_key,
		"X-RapidAPI-Host": "moviesdatabase.p.rapidapi.com"
	}

	response = requests.request("GET", url, headers=headers, params=query)

	request_back += 25

# form json
movie_json = json.loads(response.text)

# create and set up dataframe from json
df = pd.json_normalize(movie_json,"results", max_level=2)

columns = ["id", "titleType.id", "titleType.isSeries", "titleType.isEpisode", "titleText.text",	"releaseDate.day",	"releaseDate.month", "releaseDate.year", "primaryImage.url"]
current_datetime = datetime.now()

movies_table = df[columns]

#create release date and sync date fields
movies_table["RELEASE_DATE"] = movies_table[["releaseDate.year", "releaseDate.month", "releaseDate.day"]].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns")
movies_table["RELEASE_DATE"] = pd.to_datetime(movies_table["RELEASE_DATE"])

movies_table["_SYNC_DATE"] = current_datetime

#drop and rename fields
movies_table.drop(columns=["releaseDate.year", "releaseDate.month", "releaseDate.day"],inplace=True)
movies_table.rename(columns={"id": "MOVIE_ID", "titleType.id": "TITLE_TYPE_ID", "titleType.isSeries": "IS_SERIES", "titleType.isEpisode": "IS_EPISODE", "titleText.text": "TITLE", "primaryImage.url": "IMAGE_URL"}, inplace=True)

movies_table.to_csv('movies.csv', index=False) 

#upload to s3 bucket
s3 = boto3.client('s3',
         aws_access_key_id=aws_key,
         aws_secret_access_key= aws_secret)
    
with open("movies.csv", "rb") as file:
    s3.upload_fileobj(file, "myproject-imdb", "movies.csv")

#connect to snowflake and copy data from s3 bucket to snowflake
snowflake_credentials = 'snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}'.format(
	user=user_name,
	password=user_password,
	account_identifier=wh_account_identifier,
	database_name ='IMDB',
	schema_name = 'PUBLIC'
)

engine = sql.create_engine(snowflake_credentials)
connect = engine.connect()

connect.execute(f''' COPY INTO movies
					FROM s3://myproject-imdb/movies.csv credentials=(AWS_KEY_ID='{aws_key}' AWS_SECRET_KEY='{aws_secret}')
					FILE_FORMAT = (type = csv field_optionally_enclosed_by='"' SKIP_HEADER = 1);''')
	
connect.close()
engine.dispose()
	
