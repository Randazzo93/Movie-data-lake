import requests
import Movie_id_scrape as movies
import os
import json

url = "https://moviesdatabase.p.rapidapi.com/titles/x/titles-by-ids"
secret_key = os.getenv('SECRET_KEY')

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

# Materialize as dictionary
movie_json = json.loads(response.text)

# Serializing json
json_object = json.dumps(movie_json, indent=4)
 
# Writing to sample.json
with open("sample.json", "w") as outfile:
    outfile.write(json_object)