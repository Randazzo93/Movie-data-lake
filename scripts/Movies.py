import requests
import Movie_id_scrape as movies

url = "https://moviesdatabase.p.rapidapi.com/titles/x/titles-by-ids"

movie_id_list = movies.get_movie_id()

request_back = 0
runs = 0

while len(movie_id_list)/25 > runs:
    
	runs += 1

	if(request_back == 0):
		movie_ids = (','.join(movie_id_list[0:24]))
	else:
		end_list = request_back + 24
		movie_ids = (','.join(movie_id_list[request_back:end_list]))
	query_movie_ids = '"' + movie_ids + '"'

	querystring = {"idsList":query_movie_ids}

	headers = {
		"X-RapidAPI-Key": "f2e66c496cmsh0b2d77a1286a389p114e69jsn6e6dd548a048",
		"X-RapidAPI-Host": "moviesdatabase.p.rapidapi.com"
	}

	response = requests.request("GET", url, headers=headers, params=querystring)

	request_back += 25
	print(response.text)