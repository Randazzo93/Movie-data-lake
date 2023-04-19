import requests
import movie_id_scrape as movies
import os
import json
import boto3
import pandas as pd
from datetime import datetime

url = "https://moviesdatabase.p.rapidapi.com/titles/x/titles-by-ids"
secret_key = os.getenv('SECRET_KEY')
aws_key = os.getenv('AWS_KEY')
aws_secret = os.getenv('AWS_SECRET')

movie_id_list = movies.get_movie_id()

request_back = 0
runs = 0

# Most request that can be made at 1 time is 25
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
movies_table["releaseDate"] = movies_table[["releaseDate.year",	"releaseDate.month", "releaseDate.day"]].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns")
movies_table["releaseDate"] = pd.to_datetime(movies_table["releaseDate"])

movies_table["_syncDate"] = current_datetime

movies_table.drop(columns=["releaseDate.year", "releaseDate.month", "releaseDate.day"],inplace=True)
movies_table.rename(columns={"id": "movie_id", "titleType.id": "title_type_id", "titleType.isSeries": "isSeries", "titleType.isEpisode": "isEpisode", "titleText.text": "title", "primaryImage.url": "image_url"}, inplace=True)

movies_table.to_csv('movies.csv', index=False) 

s3 = boto3.client('s3',
         aws_access_key_id=aws_key,
         aws_secret_access_key= aws_secret)
    
#upload to s3 bucket
with open("movies.csv", "rb") as file:
    s3.upload_fileobj(file, "myproject-imdb", "movies.csv")