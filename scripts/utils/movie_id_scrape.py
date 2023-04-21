from requests import get
from bs4 import BeautifulSoup
import re
import math
from time import time, sleep
from random import randint
from IPython.core.display import clear_output
from warnings import warn
import sys

def inter(s):
    return s.format(**sys._getframe(1).f_locals)

def get_url(start: int) -> str:
    url = "https://www.imdb.com/search/title/?title_type=feature&num_votes=25000,&view=simple&sort=num_votes,desc&start={start}&ref_=adv_nxt"
    return url

def get_movie_id ():
    # url = "https://www.imdb.com/search/title/?title_type=feature&num_votes=25000,&view=simple&sort=num_votes,desc&start=0&ref_=adv_nxt"
    # response = get(url)
    # html_soup = BeautifulSoup(response.text, 'html.parser')

    # num_films_text = html_soup.find_all('div', class_ = 'desc')
    # num_films=re.search('of (\d.+) titles',str(num_films_text[0])).group(1)
    # num_films=int(num_films.replace(',', ''))

    # num_pages = math.ceil(num_films/50)

    ids = []
    start_time = time()
    requests = 0
    films = 0

    # For every page in the interval`
    for page in range(1,4):    
        # Make a get request
        if page == 1:
            url = get_url(51)
        elif page == 2:
            films = films + 51
            url = get_url(101)
        else:
            films = films + 50
            url = get_url(151)
            
        response = get(url)

        # Pause the loop
        sleep(randint(8,15))  

        # Monitor the requests
        requests += 1
        sleep(randint(1,3))
        elapsed_time = time() - start_time
        print('Request: {}; Frequency: {} requests/s'.format(requests, requests/elapsed_time))
        clear_output(wait = True) 

        # Throw a warning for non-200 status codes
        if response.status_code != 200:
            warn('Request: {}; Status code: {}'.format(requests, response.status_code))   

        # Break the loop if the number of requests is greater than expected
        # if requests > num_pages:
        #     warn('Number of requests was greater than expected.')  
        #     break

        print(get_url(films))
        # Parse the content of the request with BeautifulSoup
        page_html = BeautifulSoup(response.text, 'html.parser')

        # Select all the 50 movie containers from a single page
        movie_containers = page_html.find_all('div', class_ = 'lister-item mode-simple')

        # Scrape the ID 
        for index in range(len(movie_containers)):
            id = re.search('tt(\d+)/',str(movie_containers[index].a)).group(1)
            ids.append('tt' + id)
    return ids

movies = get_movie_id()
print(len(movies))
print(len(set(movies)))
print(movies)