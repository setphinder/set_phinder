# -*- coding: utf-8 -*-
"""
Created on Tue Jul 26 14:44:25 2022

@author: Benjamin Walworth

This file looks up the latitude and longitude for each venue. If it is not able to do that, we search for city, then state, and so on.

"""

## import needed modules
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
import os
import json

## set directory for reading config
project_dir = "C:\\Users\\benwa\\Documents\\projects\\set_phinder"
os.chdir(project_dir) 

## read in config
with open('config.json', 'r') as openfile:
    # Reading from json file
    config = json.load(openfile)
    

## read in venues
os.chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["raw"])
venues = pd.read_csv("venues.csv")


os.chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["interim"])
found_venues = pd.read_csv("venues_geo.csv")

## Set those we know are unknown
venues.loc[venues["city"]=="Outer Burlington", ["city"]] = "Burlington"
venues.loc[venues["venuename"]=="Unknown Venue", ["venuename"]] = "Unknown"
venues.loc[venues["venuename"]=="BAD VENUE", ["venuename"]] = "Unknown"
venues.loc[venues["venuename"]=="BAD VENUE REUSE LATER", ["venuename"]] = "Unknown"

## Drop all missing venues
venues = venues.loc[venues["city"]!="Unknown", :].reset_index(drop=True)

## Create an value to look up regarding the venue based on what is available.
venues["lookup_id"] = np.where(venues["state"].notnull(), 
                               np.where(venues["venuename"]=="Unknown", 
                                        venues["city"]+", "+venues["state"]+", "+venues["country"],
                                        venues["venuename"]+", "+venues["city"]+", "+venues["state"]+", "+venues["country"]),
                               np.where(venues["venuename"]=="Unknown", 
                                        venues["city"]+", "+venues["country"],
                                        venues["venuename"]+", "+venues["city"]+", "+venues["country"]))                               

## We make sure that id is not missing
venues = venues.loc[venues["lookup_id"].notnull(), :].reset_index(drop=True)

## Create a secondary id to look up in the event we do not find results on the first.
venues["lookup_id2"] = np.where(venues["state"].notnull(), 
                               venues["city"]+", "+venues["state"]+", "+venues["country"],
                               venues["city"]+", "+venues["country"])                               


found_venues = found_venues.loc[found_venues["latitude"].notnull(), :].reset_index(drop=True)

venues = venues.loc[~venues["venueid"].isin(found_venues["venueid"].unique().tolist()), :].reset_index(drop=True)


## initialize the geosearch tool
geolocator = Nominatim(user_agent="phish_phinder")

## Set all to none to start
venues["latitude"] = None
venues["longitude"] = None

## For each venue, we perform a search
for col_id in venues["venueid"].unique().tolist():
    lookup_id = venues.loc[venues["venueid"]==col_id, ["lookup_id"]]["lookup_id"].values[0]
    # time.sleep(random.randint(0, 3))
    try:
        location = geolocator.geocode(lookup_id)
    except:
        # time.sleep(random.randint(0, 3))
        lookup_id = venues.loc[venues["venueid"]==col_id, ["lookup_id2"]]["lookup_id2"].values[0]
        location = geolocator.geocode(lookup_id)            
    if location is None:
        try:
            lookup_id = venues.loc[venues["venueid"]==col_id, ["lookup_id2"]]["lookup_id2"].values[0]
            location = geolocator.geocode(lookup_id)    
            if location is None:
                venues.loc[venues["venueid"]==col_id, ["latitude"]] = None
                venues.loc[venues["venueid"]==col_id, ["longitude"]] = None
            else:    
                venues.loc[venues["venueid"]==col_id, ["latitude"]] = location.latitude
                venues.loc[venues["venueid"]==col_id, ["longitude"]] = location.longitude
        except:
            venues.loc[venues["venueid"]==col_id, ["latitude"]] = None
            venues.loc[venues["venueid"]==col_id, ["longitude"]] = None           
    else:
        venues.loc[venues["venueid"]==col_id, ["latitude"]] = location.latitude
        venues.loc[venues["venueid"]==col_id, ["longitude"]] = location.longitude   

## Export the locations

venues = pd.concat([found_venues, venues], ignore_index=True)

os.chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["interim"])
venues.to_csv("venues_geo.csv", index=False)
