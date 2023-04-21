import os
import requests
import pandas as pd
import json
import utils.movie_id_scrape as movies
import utils.snowflake_connection as sc
import utils.load_s3_bucket as s3
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

dotenv_path = Path('env/.env')
load_dotenv(dotenv_path=dotenv_path)

#get enviroment variables
aws_key = os.getenv('AWS_KEY')
aws_secret = os.getenv('AWS_SECRET')
secret_key = os.getenv('SECRET_KEY')

def request_data():
	#get movie id from IMBD
	url = "https://moviesdatabase.p.rapidapi.com/titles/x/titles-by-ids"
	movie_id_list = movies.get_movie_id()

	request_back = 0
	runs = 0
	combined_request=[]

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
		combined_request.append(movie_json)

	return combined_request

def gather_and_transform_data():
	movie_json = request_data()
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

	movies_table.to_csv('csv/movies.csv', index=False) 

def main():

	gather_and_transform_data()
	s3.load_s3_bucket("movies.csv")
	sc.query_warehouse(f''' COPY INTO movies
                        FROM s3://myproject-imdb/movies.csv credentials=(AWS_KEY_ID='{aws_key}' AWS_SECRET_KEY='{aws_secret}')
                        FILE_FORMAT = (type = csv field_optionally_enclosed_by='"' SKIP_HEADER = 1)''', "movies")

if __name__ == "__main__":
    main()