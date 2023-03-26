# -*- coding: utf-8 -*-
"""
Created on Tue Jul 26 12:24:14 2022

@author: Benjamin Walworth

This file pulls down data from phish.net
"""
import os
import requests
import pandas as pd
import json



## can set this up with relative directories, but doing this for simplicity of running in console for myself.
project_dir = "C:\\Users\\benwa\\Documents\\projects\\set_phinder"
os.chdir(project_dir) 

## read in config
with open('config.json', 'r') as openfile:
    # Reading from json file
    config = json.load(openfile)

## Get API key and API URL
url = "https://api.phish.net/v5/"
apikey = config["phish_api_key"]

## Get Venues
response = requests.get(url+"venues"+".json"+"?"+"apikey="+apikey)
venues = pd.DataFrame(response.json()["data"])

## Get Shows
response = requests.get(url+"shows"+".json"+"?"+"apikey="+apikey)
shows = pd.DataFrame(response.json()["data"])

## Get setlists
response = requests.get(url+"setlists"+".json"+"?"+"apikey="+apikey)
setlists = pd.DataFrame(response.json()["data"])

## Get songs
response = requests.get(url+"songs"+".json"+"?"+"apikey="+apikey)
songs = pd.DataFrame(response.json()["data"])

## Get songdata
response = requests.get(url+"songdata"+".json"+"?"+"apikey="+apikey)
songdata = pd.DataFrame(response.json()["data"])

## Get jamdata
response = requests.get(url+"jamcharts"+".json"+"?"+"apikey="+apikey)
jamcharts = pd.DataFrame(response.json()["data"])


## Change directory to export datasets.
os.chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["raw"])

venues.to_csv("venues.csv", index=False)
shows.to_csv("shows.csv", index=False)
setlists.to_csv("setlists.csv", index=False)
songs.to_csv("songs.csv", index=False)
jamcharts.to_csv("jamcharts.csv", index=False)

