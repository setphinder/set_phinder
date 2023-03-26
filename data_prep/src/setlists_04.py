# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 09:15:06 2022

@author: Benjamin Walworth

This file gets all song charactersitics and generates our units of observation by working with the set lists. 

"""

## import modules needed
import pandas as pd
import numpy as np
import os
import json

## Get the configuration file needed
project_dir = "C:\\Users\\benwa\\Documents\\projects\\set_phinder"
os.chdir(project_dir) 

## read in config
with open('config.json', 'r') as openfile:
    # Reading from json file
    config = json.load(openfile)


## Set the directory for reading raw data
os.chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["raw"])


## Read in Shows
shows = pd.read_csv("shows.csv")
## Read in All Songs
songs = pd.read_csv("songs.csv")
## Read in Setlists
setlist = pd.read_csv("setlists.csv")
## Read in Albums. This is mannually created
albums = pd.read_excel("albums.xlsx", sheet_name=0)

## Read in Cleaned Shows
os.chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["interim"])
cleaned_shows = pd.read_csv("show_venue_characteristics.csv")

## Convert to date
shows["showdate"] = pd.to_datetime(shows['showdate'])

## Sort and subset to phish shows and remove the excluded shows
shows = shows.sort_values(by=["venueid", "showdate", "showid"]).reset_index(drop=True)
phish_shows = shows.loc[shows["artistid"]==1, :].reset_index(drop=True)
phish_shows = phish_shows.loc[phish_shows["exclude_from_stats"]==0, :].reset_index(drop=True)

## merge to clearned venues
phish_shows = phish_shows.merge(cleaned_shows.loc[:, ["showid", "runid", "new_tourid"]], how="left", on="showid")

## get the cleaned state version
phish_shows["state"] = np.where(phish_shows["state"].isnull(), phish_shows["country"], phish_shows["state"])

## export the fixed state info
phish_shows.to_csv("show_tourid_statefix.csv", index=False)

## dictionary for phish affiliates. Anything produced by an individual member will be attributed as a "Phish" son and not a cover
dict_to_map = {"Vida Blue":"Phish", "Trey, Mike and The Duo":"Phish", "Trey Anastasio & Don Hart":"Phish", "Trey Anastasio":"Phish", "The Dude of Life (with Phish)":"Phish", 
  "Page McConnell":"Phish", "Mike Gordon and Leo Kottke":"Phish", "Mike Gordon":"Phish", "Ghosts of the Forest":"Phish", "Amfibian":"Phish", "Bivouac Jaun":"Phish", "Surrender to the Air":"Phish", "SerialPod":"Phish"}


## merge shows to the set lists to just get phish sets
phish_sets = phish_shows.loc[:, ["showid", "showdate", "venueid", "new_tourid", "runid", "state"]].merge(setlist.loc[:, ["showid", "uniqueid", "songid", "is_original"]], how="inner", on=["showid"])

## here we get the max show which we have a set for. This is the end of "training" data 
max_show = phish_sets.loc[:, ["showid", "showdate", "venueid", "new_tourid", "runid", "state"]].sort_values(["showdate", "showid", "venueid", "new_tourid", "runid", "state"]).reset_index(drop=True)
max_show = max_show.drop_duplicates(subset=["showdate"], keep="last").reset_index(drop=True)
max_show = max_show[["showdate"]].max()[0]

## set that date
phish_shows['max_show'] = max_show

## grab the next show. 
upcomming_shows = phish_shows.loc[phish_shows["showdate"]>phish_shows["max_show"], :].reset_index(drop=True)
upcomming_shows = upcomming_shows.sort_values(["showdate", "showid", "venueid", "new_tourid", "runid", "state"]).reset_index(drop=True)
upcomming_shows = upcomming_shows.loc[upcomming_shows.index==0, ["showid", "showdate", "venueid", "new_tourid", "runid", "state"]].reset_index(drop=True)

## merge the albums over to sets
phish_sets = phish_sets.merge(albums.loc[:, ["songid", "albumid"]].drop_duplicates(["songid"]).reset_index(drop=True), how="left", on="songid")

## check if it says the song is not original, if there is an album, we say it is original.
phish_sets["is_cover"] = np.where(phish_sets["is_original"]==0, np.where(phish_sets["albumid"].notnull(), 0, 1), 0)

## map the artists to phish artistis
songs["new_artist"] = songs['artist'].map(dict_to_map).fillna(songs['artist'])
phish_songs = songs.loc[songs["new_artist"]=="Phish",:].reset_index(drop=True)
phish_sets = phish_sets.merge(phish_songs.loc[:, ["songid", "new_artist"]].drop_duplicates(["songid"]).reset_index(drop=True), how="left", on="songid")

## if it is played by one of the affiliates, we do not consider it a cover for modeling purposes.
phish_sets["is_cover"] = np.where(phish_sets["is_cover"]==1, np.where(phish_sets["new_artist"].notnull(), 0, 1), 0)

phish_sets = phish_sets.drop(columns=["new_artist", "albumid", "is_original"])

## don't allow multiple songs per night. Ie, no tweezer fest for modeling.
phish_sets = phish_sets.sort_values(by=["runid", "showdate", "showid"]).reset_index(drop=True)
phish_sets = phish_sets.drop_duplicates(["runid", "showdate", "showid", "songid"]).reset_index(drop=True)

del dict_to_map, phish_shows, phish_songs, max_show

## set our outcome variable "played"
phish_sets["played"] = 1


#### These are all the variables as of when they are played.


## get number of times a song has been played on any given show
phish_sets = phish_sets.sort_values(["songid", "showdate", "showid"]).reset_index(drop=True)
phish_sets["num_times_played_song"] = phish_sets.groupby(by=["songid"]).cumcount()
phish_sets["num_times_played_song"] = phish_sets["num_times_played_song"]+1

## get time since the song was last played in days. We will recalculate this later, as we don't use this other than calculating min/max gaps.
phish_sets['last_played_song'] = phish_sets.groupby(["songid"])["showdate"].shift(1)
phish_sets['time_last_played_song'] = phish_sets["showdate"] - phish_sets['last_played_song']
phish_sets['time_last_played_song'] = phish_sets['time_last_played_song'].dt.days

## Get gaps cummulatively, min, max, average and median. This is important because these are PLAYED. so we will have theses cumulatively, when they are played.
phish_sets['min_gap_song'] = phish_sets.groupby(["songid"])['time_last_played_song'].cummin()
phish_sets['max_gap_song'] = phish_sets.groupby(["songid"])['time_last_played_song'].cummax()
phish_sets['avg_gap_song'] = phish_sets.groupby(["songid"])['time_last_played_song'].apply(lambda x: x.expanding().mean())
phish_sets['median_gap_song'] = phish_sets.groupby(["songid"])['time_last_played_song'].apply(lambda x: x.expanding().median())


## Get the number of times a song is played at a given venue
phish_sets = phish_sets.sort_values(["venueid", "songid", "showdate", "showid"]).reset_index(drop=True)
phish_sets["num_times_played_song_venue"] = phish_sets.groupby(by=["venueid", "songid"]).cumcount()
phish_sets["num_times_played_song_venue"] = phish_sets["num_times_played_song_venue"]+1

## Get the time since last played at the venue to get the gaps for the venue
phish_sets['last_played_song_venue'] = phish_sets.groupby(["venueid", "songid"])["showdate"].shift(1)
phish_sets['time_last_played_song_venue'] = phish_sets["showdate"] - phish_sets['last_played_song_venue']
phish_sets['time_last_played_song_venue'] = phish_sets['time_last_played_song_venue'].dt.days


phish_sets['min_gap_song_venue'] = phish_sets.groupby(["venueid", "songid"])['time_last_played_song_venue'].cummin()
phish_sets['max_gap_song_venue'] = phish_sets.groupby(["venueid", "songid"])['time_last_played_song_venue'].cummax()
phish_sets['avg_gap_song_venue'] = phish_sets.groupby(["venueid", "songid"])['time_last_played_song_venue'].apply(lambda x: x.expanding().mean())
phish_sets['median_gap_song_venue'] = phish_sets.groupby(["venueid", "songid"])['time_last_played_song_venue'].apply(lambda x: x.expanding().median())


## Repeat but for the run. Doubt this will be helpful
phish_sets = phish_sets.sort_values(["runid", "songid", "showdate", "showid"]).reset_index(drop=True)
phish_sets["num_times_played_song_run"] = phish_sets.groupby(by=["runid", "songid"]).cumcount()
phish_sets["num_times_played_song_run"] = phish_sets["num_times_played_song_run"]+1



phish_sets['last_played_song_run'] = phish_sets.groupby(["runid", "songid"])["showdate"].shift(1)
phish_sets['time_last_played_song_run'] = phish_sets["showdate"] - phish_sets['last_played_song_run']
phish_sets['time_last_played_song_run'] = phish_sets['time_last_played_song_run'].dt.days

phish_sets['min_gap_song_run'] = phish_sets.groupby(["runid", "songid"])['time_last_played_song_run'].cummin()
phish_sets['max_gap_song_run'] = phish_sets.groupby(["runid", "songid"])['time_last_played_song_run'].cummax()
phish_sets['avg_gap_song_run'] = phish_sets.groupby(["runid", "songid"])['time_last_played_song_run'].apply(lambda x: x.expanding().mean())
phish_sets['median_gap_song_run'] = phish_sets.groupby(["runid", "songid"])['time_last_played_song_run'].apply(lambda x: x.expanding().median())


## Repeat for tour. 


phish_sets = phish_sets.sort_values(["new_tourid", "songid", "showdate", "showid"]).reset_index(drop=True)
phish_sets["num_times_played_song_tour"] = phish_sets.groupby(by=["new_tourid", "songid"]).cumcount()
phish_sets["num_times_played_song_tour"] = phish_sets["num_times_played_song_tour"]+1



phish_sets['last_played_song_tour'] = phish_sets.groupby(["new_tourid", "songid"])["showdate"].shift(1)
phish_sets['time_last_played_song_tour'] = phish_sets["showdate"] - phish_sets['last_played_song_tour']
phish_sets['time_last_played_song_tour'] = phish_sets['time_last_played_song_tour'].dt.days

phish_sets['min_gap_song_tour'] = phish_sets.groupby(["new_tourid", "songid"])['time_last_played_song_tour'].cummin()
phish_sets['max_gap_song_tour'] = phish_sets.groupby(["new_tourid", "songid"])['time_last_played_song_tour'].cummax()
phish_sets['avg_gap_song_tour'] = phish_sets.groupby(["new_tourid", "songid"])['time_last_played_song_tour'].apply(lambda x: x.expanding().mean())
phish_sets['median_gap_song_tour'] = phish_sets.groupby(["new_tourid", "songid"])['time_last_played_song_tour'].apply(lambda x: x.expanding().median())


## Repeat for state


phish_sets = phish_sets.sort_values(by=["state", "songid", "showdate", "showid"]).reset_index(drop=True)
phish_sets["num_times_played_song_state"] = phish_sets.groupby(by=["state", "songid"]).cumcount()
phish_sets["num_times_played_song_state"] = phish_sets["num_times_played_song_state"]+1


phish_sets['last_played_song_state'] = phish_sets.groupby(["state", "songid"])["showdate"].shift(1)
phish_sets['time_last_played_song_state'] = phish_sets["showdate"] - phish_sets['last_played_song_state']
phish_sets['time_last_played_song_state'] = phish_sets['time_last_played_song_state'].dt.days

phish_sets['min_gap_song_state'] = phish_sets.groupby(["state", "songid"])['time_last_played_song_state'].cummin()
phish_sets['max_gap_song_state'] = phish_sets.groupby(["state", "songid"])['time_last_played_song_state'].cummax()
phish_sets['avg_gap_song_state'] = phish_sets.groupby(["state", "songid"])['time_last_played_song_state'].apply(lambda x: x.expanding().mean())
phish_sets['median_gap_song_state'] = phish_sets.groupby(["state", "songid"])['time_last_played_song_state'].apply(lambda x: x.expanding().median())


#### The other option that might be good is within a calendar year.

## drop the time last played and last played dates
phish_sets = phish_sets.drop(columns=['last_played_song', 'time_last_played_song', 'last_played_song_venue', 'time_last_played_song_venue', 'last_played_song_run', 'time_last_played_song_run', 'last_played_song_tour', 'time_last_played_song_tour', 'last_played_song_state', 'time_last_played_song_state'])


phish_sets = phish_sets.sort_values(["showdate", "showid", "songid"]).reset_index(drop=True)

phish_sets["show_dummy_date"] = phish_sets["showdate"].copy()
phish_sets["played"] = 1


## Get the first time the song was played
debut = phish_sets.loc[phish_sets["num_times_played_song"]==1, :].reset_index(drop=True)

debut = debut.drop_duplicates(["songid"]).reset_index(drop=True)
debut = debut.loc[:, ["songid", "showdate"]].rename(columns={"showdate":"song_debut_date"})

covers = phish_sets.drop_duplicates(["songid"]).reset_index(drop=True)
covers = covers.loc[:, ["songid", "is_cover"]]

## export if a song was a cover, just for potential use
covers.to_csv("covers.csv", index=False)


## GEt data for creating the panel of songs
shows_for_cj = phish_sets.loc[:, ["showid", "showdate", "new_tourid", "state", "venueid", "runid"]].drop_duplicates(["showid"]).reset_index(drop=True)

## here we add in the new show
date_of_new_show = upcomming_shows["showdate"][0]
shows_for_cj = pd.concat([shows_for_cj, upcomming_shows.loc[:, ["showid", "showdate", "new_tourid", "state", "venueid", "runid"]].drop_duplicates(["showid"]).reset_index(drop=True)])

shows_for_cj['key'] = 0
debut['key'] = 0


####################



## Perform cross join to get units of observation
units_of_observation = shows_for_cj.merge(debut, how="outer", on="key")

del shows_for_cj, debut

## Only get those that are on or after the debut of the song.
units_of_observation = units_of_observation.loc[units_of_observation["showdate"]>=units_of_observation["song_debut_date"], :].reset_index(drop=True)


## Here we get number of times played per tour
phish_sets = phish_sets.sort_values(["new_tourid", "songid", "showdate", "showid"]).reset_index(drop=True)
num_times_played_last_tour = phish_sets.groupby(["new_tourid", "songid"])["played"].sum().reset_index().rename(columns={'played':'num_times_song_played_last_tour'})

phish_sets["key"] = 0

## This is then merged to units of observation. If non played the count is 0
num_times_played_last_tour = units_of_observation.loc[:, ["new_tourid", "songid"]].drop_duplicates().reset_index(drop=True).merge(num_times_played_last_tour, how="left", on=["new_tourid", "songid"]).fillna(value=0)

## Sort by tour and song
num_times_played_last_tour =  num_times_played_last_tour.sort_values(["new_tourid", "songid"]).reset_index(drop=True)

## For each song, we then get the min/max/average over all tours for the number of times played.
num_times_played_last_tour['min_num_times_song_played_tour'] = num_times_played_last_tour.groupby(["songid"])['num_times_song_played_last_tour'].cummin()
num_times_played_last_tour['max_num_times_song_played_tour'] = num_times_played_last_tour.groupby(["songid"])['num_times_song_played_last_tour'].cummax()
num_times_played_last_tour['avg_num_times_song_played_tour'] = num_times_played_last_tour.groupby(["songid"])['num_times_song_played_last_tour'].apply(lambda x: x.expanding().mean())
num_times_played_last_tour['median_num_times_song_played_tour'] = num_times_played_last_tour.groupby(["songid"])['num_times_song_played_last_tour'].apply(lambda x: x.expanding().median())


## Repeat for tour and state


num_times_played_last_tour_state = phish_sets.groupby(["new_tourid","state", "songid"])["played"].sum().reset_index().rename(columns={'played':'num_times_song_played_state_last_tour'})


num_times_played_last_tour_state = units_of_observation.loc[:, ["new_tourid", "songid", "state"]].drop_duplicates().reset_index(drop=True).merge(num_times_played_last_tour_state, how="left", on=["new_tourid", "songid", "state"]).fillna(value=0)

num_times_played_last_tour_state =  num_times_played_last_tour_state.sort_values(["state", "songid", "new_tourid"]).reset_index(drop=True)

num_times_played_last_tour_state['min_num_times_song_played_state_tour'] = num_times_played_last_tour_state.groupby(["state","songid"])['num_times_song_played_state_last_tour'].cummin()
num_times_played_last_tour_state['max_num_times_song_played_state_tour'] = num_times_played_last_tour_state.groupby(["state","songid"])['num_times_song_played_state_last_tour'].cummax()
num_times_played_last_tour_state['avg_num_times_song_played_state_tour'] = num_times_played_last_tour_state.groupby(["state","songid"])['num_times_song_played_state_last_tour'].apply(lambda x: x.expanding().mean())
num_times_played_last_tour_state['median_num_times_song_played_state_tour'] = num_times_played_last_tour_state.groupby(["state","songid"])['num_times_song_played_state_last_tour'].apply(lambda x: x.expanding().median())



num_times_played_last_tour_venue = phish_sets.groupby(["new_tourid", "venueid", "songid"])["played"].sum().reset_index().rename(columns={'played':'num_times_song_played_venue_last_tour'})

# phish_tour_sets = phish_sets.loc[:, ["new_tourid", "venueid", "key"]].drop_duplicates().reset_index(drop=True).merge(phish_sets.loc[:, ["songid", "key"]].drop_duplicates().reset_index(drop=True), how="outer", on="key").drop(columns=["key"])

num_times_played_last_tour_venue = units_of_observation.loc[:, ["new_tourid", "songid", "venueid"]].drop_duplicates().reset_index(drop=True).merge(num_times_played_last_tour_venue, how="left", on=["new_tourid", "songid", "venueid"]).fillna(value=0)

num_times_played_last_tour_venue =  num_times_played_last_tour_venue.sort_values(["venueid", "songid", "new_tourid"]).reset_index(drop=True)

num_times_played_last_tour_venue['min_num_times_song_played_venue_tour'] = num_times_played_last_tour_venue.groupby(["venueid","songid"])['num_times_song_played_venue_last_tour'].cummin()
num_times_played_last_tour_venue['max_num_times_song_played_venue_tour'] = num_times_played_last_tour_venue.groupby(["venueid","songid"])['num_times_song_played_venue_last_tour'].cummax()
num_times_played_last_tour_venue['avg_num_times_song_played_venue_tour'] = num_times_played_last_tour_venue.groupby(["venueid","songid"])['num_times_song_played_venue_last_tour'].apply(lambda x: x.expanding().mean())
num_times_played_last_tour_venue['median_num_times_song_played_venue_tour'] = num_times_played_last_tour_venue.groupby(["venueid","songid"])['num_times_song_played_venue_last_tour'].apply(lambda x: x.expanding().median())




## This gives us num times played last tour and min/max played ANY tour
num_times_played_last_tour = num_times_played_last_tour.sort_values(["new_tourid", "songid"]).reset_index(drop=True)
for col in ['num_times_song_played_last_tour', 'min_num_times_song_played_tour', 'max_num_times_song_played_tour', 'avg_num_times_song_played_tour', 'median_num_times_song_played_tour']:
    num_times_played_last_tour[col] = num_times_played_last_tour.groupby(["songid"])[col].shift(1)   
    num_times_played_last_tour[col] = num_times_played_last_tour.groupby(["songid"])[col].fillna(method="ffill")

num_times_played_last_tour_venue = num_times_played_last_tour_venue.sort_values(["new_tourid", "venueid", "songid"]).reset_index(drop=True)
for col in ['num_times_song_played_venue_last_tour', 'min_num_times_song_played_venue_tour', 'max_num_times_song_played_venue_tour', 'avg_num_times_song_played_venue_tour', 'median_num_times_song_played_venue_tour']:
    num_times_played_last_tour_venue[col] = num_times_played_last_tour_venue.groupby(["venueid","songid"])[col].shift(1)   
    num_times_played_last_tour_venue[col] = num_times_played_last_tour_venue.groupby(["venueid","songid"])[col].fillna(method="ffill")
   

num_times_played_last_tour_state = num_times_played_last_tour_state.sort_values(["new_tourid", "state", "songid"]).reset_index(drop=True)
for col in ['num_times_song_played_state_last_tour', 'min_num_times_song_played_state_tour', 'max_num_times_song_played_state_tour', 'avg_num_times_song_played_state_tour', 'median_num_times_song_played_state_tour']:
    num_times_played_last_tour_state[col] = num_times_played_last_tour_state.groupby(["state","songid"])[col].shift(1)   
    num_times_played_last_tour_state[col] = num_times_played_last_tour_state.groupby(["state","songid"])[col].fillna(method="ffill")
   



units_of_observation = units_of_observation.merge(num_times_played_last_tour, how="left", on=["new_tourid", "songid"])
units_of_observation = units_of_observation.merge(num_times_played_last_tour_state, how="left", on=["new_tourid", "songid", "state"])
units_of_observation = units_of_observation.merge(num_times_played_last_tour_venue, how="left", on=["new_tourid", "songid", "venueid"])

del num_times_played_last_tour, num_times_played_last_tour_state, num_times_played_last_tour_venue


### We should repeat the above for GAPS. I think that will be predictive. Additionally do for gaps by CY 


###########################################################################

## here we generate the albums data.
albums = albums.sort_values(["releasedate", "albumid"]).reset_index(drop=True)

albums_count = albums.drop_duplicates(["albumid"]).reset_index(drop=True)
albums_count = albums_count.reset_index(drop=False).rename(columns={"index":"album_num"})
albums = albums.merge(albums_count.loc[:, ["albumid", "album_num"]], how="left", on="albumid")


albums = units_of_observation.merge(albums.loc[:, ["songid", "album_num", "releasedate"]], how="inner", on="songid")
albums = albums.loc[albums["releasedate"]<=albums["showdate"], :].reset_index(drop=True)
albums = albums.sort_values(["showdate", "showid", "songid", "releasedate"]).reset_index(drop=True)
albums = albums.drop_duplicates(["showid", "songid"], keep="last")

albums = albums.loc[:, ["showid", "songid", "album_num", "releasedate"]]

######################


units_of_observation = units_of_observation.merge(phish_sets.loc[:, ["showid", "songid", "show_dummy_date", "played"]], how="left", on=["showid", "songid"])


units_of_observation["played"] = units_of_observation["played"].fillna(value=0)

## Now we generate rates of being played on tour
rate_times_played_last_tour = units_of_observation.groupby(["new_tourid", "songid"])["played"].mean().reset_index().rename(columns={'played':'rate_song_played_last_tour'})


rate_times_played_last_tour =  rate_times_played_last_tour.sort_values(["new_tourid", "songid"]).reset_index(drop=True)

rate_times_played_last_tour['min_rate_song_played_tour'] = rate_times_played_last_tour.groupby(["songid"])['rate_song_played_last_tour'].cummin()
rate_times_played_last_tour['max_rate_song_played_tour'] = rate_times_played_last_tour.groupby(["songid"])['rate_song_played_last_tour'].cummax()
rate_times_played_last_tour['avg_rate_song_played_tour'] = rate_times_played_last_tour.groupby(["songid"])['rate_song_played_last_tour'].apply(lambda x: x.expanding().mean())
rate_times_played_last_tour['median_rate_song_played_tour'] = rate_times_played_last_tour.groupby(["songid"])['rate_song_played_last_tour'].apply(lambda x: x.expanding().median())



rate_times_played_last_tour_state = units_of_observation.groupby(["new_tourid","state", "songid"])["played"].mean().reset_index().rename(columns={'played':'rate_song_played_state_last_tour'})


rate_times_played_last_tour_state =  rate_times_played_last_tour_state.sort_values(["state", "songid", "new_tourid"]).reset_index(drop=True)

rate_times_played_last_tour_state['min_rate_song_played_state_tour'] = rate_times_played_last_tour_state.groupby(["state","songid"])['rate_song_played_state_last_tour'].cummin()
rate_times_played_last_tour_state['max_rate_song_played_state_tour'] = rate_times_played_last_tour_state.groupby(["state","songid"])['rate_song_played_state_last_tour'].cummax()
rate_times_played_last_tour_state['avg_rate_song_played_state_tour'] = rate_times_played_last_tour_state.groupby(["state","songid"])['rate_song_played_state_last_tour'].apply(lambda x: x.expanding().mean())
rate_times_played_last_tour_state['median_rate_song_played_state_tour'] = rate_times_played_last_tour_state.groupby(["state","songid"])['rate_song_played_state_last_tour'].apply(lambda x: x.expanding().median())



rate_times_played_last_tour_venue = units_of_observation.groupby(["new_tourid", "venueid", "songid"])["played"].mean().reset_index().rename(columns={'played':'rate_song_played_venue_last_tour'})


rate_times_played_last_tour_venue =  rate_times_played_last_tour_venue.sort_values(["venueid", "songid", "new_tourid"]).reset_index(drop=True)

rate_times_played_last_tour_venue['min_rate_song_played_venue_tour'] = rate_times_played_last_tour_venue.groupby(["venueid","songid"])['rate_song_played_venue_last_tour'].cummin()
rate_times_played_last_tour_venue['max_rate_song_played_venue_tour'] = rate_times_played_last_tour_venue.groupby(["venueid","songid"])['rate_song_played_venue_last_tour'].cummax()
rate_times_played_last_tour_venue['avg_rate_song_played_venue_tour'] = rate_times_played_last_tour_venue.groupby(["venueid","songid"])['rate_song_played_venue_last_tour'].apply(lambda x: x.expanding().mean())
rate_times_played_last_tour_venue['median_rate_song_played_venue_tour'] = rate_times_played_last_tour_venue.groupby(["venueid","songid"])['rate_song_played_venue_last_tour'].apply(lambda x: x.expanding().median())


rate_times_played_last_tour = rate_times_played_last_tour.sort_values(["new_tourid", "songid"]).reset_index(drop=True)
for col in ['rate_song_played_last_tour', 'min_rate_song_played_tour', 'max_rate_song_played_tour', 'avg_rate_song_played_tour', 'median_rate_song_played_tour']:
    rate_times_played_last_tour[col] = rate_times_played_last_tour.groupby(["songid"])[col].shift(1)   
    rate_times_played_last_tour[col] = rate_times_played_last_tour.groupby(["songid"])[col].fillna(method="ffill")

rate_times_played_last_tour_venue = rate_times_played_last_tour_venue.sort_values(["new_tourid", "venueid", "songid"]).reset_index(drop=True)
for col in ['rate_song_played_venue_last_tour', 'min_rate_song_played_venue_tour', 'max_rate_song_played_venue_tour', 'avg_rate_song_played_venue_tour', 'median_rate_song_played_venue_tour']:
    rate_times_played_last_tour_venue[col] = rate_times_played_last_tour_venue.groupby(["venueid","songid"])[col].shift(1)   
    rate_times_played_last_tour_venue[col] = rate_times_played_last_tour_venue.groupby(["venueid","songid"])[col].fillna(method="ffill")
   

rate_times_played_last_tour_state = rate_times_played_last_tour_state.sort_values(["new_tourid", "state", "songid"]).reset_index(drop=True)
for col in ['rate_song_played_state_last_tour', 'min_rate_song_played_state_tour', 'max_rate_song_played_state_tour', 'avg_rate_song_played_state_tour', 'median_rate_song_played_state_tour']:
    rate_times_played_last_tour_state[col] = rate_times_played_last_tour_state.groupby(["state","songid"])[col].shift(1)   
    rate_times_played_last_tour_state[col] = rate_times_played_last_tour_state.groupby(["state","songid"])[col].fillna(method="ffill")
   


units_of_observation = units_of_observation.merge(rate_times_played_last_tour, how="left", on=["new_tourid", "songid"])
units_of_observation = units_of_observation.merge(rate_times_played_last_tour_state, how="left", on=["new_tourid", "songid", "state"])
units_of_observation = units_of_observation.merge(rate_times_played_last_tour_venue, how="left", on=["new_tourid", "songid", "venueid"])

del rate_times_played_last_tour, rate_times_played_last_tour_state, rate_times_played_last_tour_venue
###########################

## Now we get the date the song was last played on any given date.
units_of_observation = units_of_observation.sort_values(["songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['last_played_song'] = units_of_observation.groupby(["songid"])["show_dummy_date"].shift(1)
units_of_observation['last_played_song'] = units_of_observation.groupby(["songid"])['last_played_song'].fillna(method="ffill")

#####

units_of_observation = units_of_observation.sort_values(["venueid", "songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['last_played_song_venue'] = units_of_observation.groupby(["venueid", "songid"])["show_dummy_date"].shift(1)
units_of_observation['last_played_song_venue'] = units_of_observation.groupby(["venueid", "songid"])['last_played_song_venue'].fillna(method="ffill")

#####

units_of_observation = units_of_observation.sort_values(["runid", "songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['last_played_song_run'] = units_of_observation.groupby(["runid", "songid"])["show_dummy_date"].shift(1)
units_of_observation['last_played_song_run'] = units_of_observation.groupby(["runid", "songid"])['last_played_song_run'].fillna(method="ffill")

#####

units_of_observation = units_of_observation.sort_values(["new_tourid", "songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['last_played_song_tour'] = units_of_observation.groupby(["new_tourid","songid"])["show_dummy_date"].shift(1)
units_of_observation['last_played_song_tour'] = units_of_observation.groupby(["new_tourid","songid"])['last_played_song_tour'].fillna(method="ffill")

units_of_observation = units_of_observation.sort_values(["state", "songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['last_played_song_state'] = units_of_observation.groupby(["state", "songid"])["show_dummy_date"].shift(1)
units_of_observation['last_played_song_state'] = units_of_observation.groupby(["state", "songid"])['last_played_song_state'].fillna(method="ffill")

#####


## Calculated the times lat played
###################

units_of_observation['time_last_played_song'] = units_of_observation['showdate'] - units_of_observation['last_played_song']
units_of_observation['time_last_played_song'] = units_of_observation['time_last_played_song'].dt.days

units_of_observation['time_last_played_song_venue'] = units_of_observation['showdate'] - units_of_observation['last_played_song_venue']
units_of_observation['time_last_played_song_venue'] = units_of_observation['time_last_played_song_venue'].dt.days

units_of_observation['time_last_played_song_run'] = units_of_observation['showdate'] - units_of_observation['last_played_song_run']
units_of_observation['time_last_played_song_run'] = units_of_observation['time_last_played_song_run'].dt.days

units_of_observation['time_last_played_song_tour'] = units_of_observation['showdate'] - units_of_observation['last_played_song_tour']
units_of_observation['time_last_played_song_tour'] = units_of_observation['time_last_played_song_tour'].dt.days

units_of_observation['time_last_played_song_state'] = units_of_observation['showdate'] - units_of_observation['last_played_song_state'] 
units_of_observation['time_last_played_song_state'] = units_of_observation['time_last_played_song_state'].dt.days


###############################



col_list = []
for v in ["", "_venue", "_run", "_tour", "_state"]:
    minlst = [m+v for m in ["min_gap_song", "max_gap_song", "avg_gap_song", "median_gap_song"]]
    col_list.extend(minlst)

units_of_observation = units_of_observation.merge(phish_sets.loc[:, ["showid", "songid", *col_list ]], how="left", on=["showid", "songid"])



##############################################


units_of_observation = units_of_observation.sort_values(["songid", "showdate", "showid"]).reset_index(drop=True)


units_of_observation['min_gap_song'] = units_of_observation.groupby(["songid"])['min_gap_song'].shift(1)
units_of_observation['max_gap_song'] = units_of_observation.groupby(["songid"])['max_gap_song'].shift(1)
units_of_observation['avg_gap_song'] = units_of_observation.groupby(["songid"])['avg_gap_song'].shift(1)
units_of_observation['median_gap_song'] = units_of_observation.groupby(["songid"])['median_gap_song'].shift(1)

units_of_observation['min_gap_song'] = units_of_observation.groupby(["songid"])['min_gap_song'].fillna(method="ffill")
units_of_observation['max_gap_song'] = units_of_observation.groupby(["songid"])['max_gap_song'].fillna(method="ffill")
units_of_observation['avg_gap_song'] = units_of_observation.groupby(["songid"])['avg_gap_song'].fillna(method="ffill")
units_of_observation['median_gap_song'] = units_of_observation.groupby(["songid"])['median_gap_song'].fillna(method="ffill")


## if missing from the data, we can use the time last played as there hasn't been a true gap yet.

units_of_observation['min_gap_song'] = units_of_observation['min_gap_song'].fillna(units_of_observation['time_last_played_song'])
units_of_observation['max_gap_song'] = units_of_observation['max_gap_song'].fillna(units_of_observation['time_last_played_song'])
units_of_observation['avg_gap_song'] = units_of_observation['avg_gap_song'].fillna(units_of_observation['time_last_played_song'])
units_of_observation['median_gap_song'] = units_of_observation['median_gap_song'].fillna(units_of_observation['time_last_played_song'])

########################################
units_of_observation = units_of_observation.sort_values(["venueid", "songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['min_gap_song_venue'] = units_of_observation.groupby(["venueid", "songid"])['min_gap_song_venue'].shift(1)
units_of_observation['max_gap_song_venue'] = units_of_observation.groupby(["venueid", "songid"])['max_gap_song_venue'].shift(1)
units_of_observation['avg_gap_song_venue'] = units_of_observation.groupby(["venueid", "songid"])['avg_gap_song_venue'].shift(1)
units_of_observation['median_gap_song_venue'] = units_of_observation.groupby(["venueid", "songid"])['median_gap_song_venue'].shift(1)


units_of_observation['min_gap_song_venue'] = units_of_observation.groupby(["venueid", "songid"])['min_gap_song_venue'].fillna(method="ffill")
units_of_observation['max_gap_song_venue'] = units_of_observation.groupby(["venueid", "songid"])['max_gap_song_venue'].fillna(method="ffill")
units_of_observation['avg_gap_song_venue'] = units_of_observation.groupby(["venueid", "songid"])['avg_gap_song_venue'].fillna(method="ffill")
units_of_observation['median_gap_song_venue'] = units_of_observation.groupby(["venueid", "songid"])['median_gap_song_venue'].fillna(method="ffill")


units_of_observation['min_gap_song_venue'] = units_of_observation['min_gap_song_venue'].fillna(units_of_observation['time_last_played_song_venue'])
units_of_observation['max_gap_song_venue'] = units_of_observation['max_gap_song_venue'].fillna(units_of_observation['time_last_played_song_venue'])
units_of_observation['avg_gap_song_venue'] = units_of_observation['avg_gap_song_venue'].fillna(units_of_observation['time_last_played_song_venue'])
units_of_observation['median_gap_song_venue'] = units_of_observation['median_gap_song_venue'].fillna(units_of_observation['time_last_played_song_venue'])


units_of_observation = units_of_observation.sort_values(["runid", "songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['min_gap_song_run'] = units_of_observation.groupby(["runid", "songid"])['min_gap_song_run'].shift(1)
units_of_observation['max_gap_song_run'] = units_of_observation.groupby(["runid", "songid"])['max_gap_song_run'].shift(1)
units_of_observation['avg_gap_song_run'] = units_of_observation.groupby(["runid", "songid"])['avg_gap_song_run'].shift(1)
units_of_observation['median_gap_song_run'] = units_of_observation.groupby(["runid", "songid"])['median_gap_song_run'].shift(1)


units_of_observation['min_gap_song_run'] = units_of_observation.groupby(["runid", "songid"])['min_gap_song_run'].fillna(method="ffill")
units_of_observation['max_gap_song_run'] = units_of_observation.groupby(["runid", "songid"])['max_gap_song_run'].fillna(method="ffill")
units_of_observation['avg_gap_song_run'] = units_of_observation.groupby(["runid", "songid"])['avg_gap_song_run'].fillna(method="ffill")
units_of_observation['median_gap_song_run'] = units_of_observation.groupby(["runid", "songid"])['median_gap_song_run'].fillna(method="ffill")


units_of_observation['min_gap_song_run'] = units_of_observation['min_gap_song_run'].fillna(units_of_observation['time_last_played_song_run'])
units_of_observation['max_gap_song_run'] = units_of_observation['max_gap_song_run'].fillna(units_of_observation['time_last_played_song_run'])
units_of_observation['avg_gap_song_run'] = units_of_observation['avg_gap_song_run'].fillna(units_of_observation['time_last_played_song_run'])
units_of_observation['median_gap_song_run'] = units_of_observation['median_gap_song_run'].fillna(units_of_observation['time_last_played_song_run'])


### within tour it gives us this information, but not any thing about the LAST tour.

units_of_observation = units_of_observation.sort_values(["new_tourid", "songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['min_gap_song_tour'] = units_of_observation.groupby(["new_tourid", "songid"])['min_gap_song_tour'].shift(1)
units_of_observation['max_gap_song_tour'] = units_of_observation.groupby(["new_tourid", "songid"])['max_gap_song_tour'].shift(1)
units_of_observation['avg_gap_song_tour'] = units_of_observation.groupby(["new_tourid", "songid"])['avg_gap_song_tour'].shift(1)
units_of_observation['median_gap_song_tour'] = units_of_observation.groupby(["new_tourid", "songid"])['median_gap_song_tour'].shift(1)


units_of_observation['min_gap_song_tour'] = units_of_observation.groupby(["new_tourid", "songid"])['min_gap_song_tour'].fillna(method="ffill")
units_of_observation['max_gap_song_tour'] = units_of_observation.groupby(["new_tourid", "songid"])['max_gap_song_tour'].fillna(method="ffill")
units_of_observation['avg_gap_song_tour'] = units_of_observation.groupby(["new_tourid", "songid"])['avg_gap_song_tour'].fillna(method="ffill")
units_of_observation['median_gap_song_tour'] = units_of_observation.groupby(["new_tourid", "songid"])['median_gap_song_tour'].fillna(method="ffill")


units_of_observation['min_gap_song_tour'] = units_of_observation['min_gap_song_tour'].fillna(units_of_observation['time_last_played_song_tour'])
units_of_observation['max_gap_song_tour'] = units_of_observation['max_gap_song_tour'].fillna(units_of_observation['time_last_played_song_tour'])
units_of_observation['avg_gap_song_tour'] = units_of_observation['avg_gap_song_tour'].fillna(units_of_observation['time_last_played_song_tour'])
units_of_observation['median_gap_song_tour'] = units_of_observation['median_gap_song_tour'].fillna(units_of_observation['time_last_played_song_tour'])



###############

units_of_observation = units_of_observation.sort_values(["state", "songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['min_gap_song_state'] = units_of_observation.groupby(["state", "songid"])['min_gap_song_state'].shift(1)
units_of_observation['max_gap_song_state'] = units_of_observation.groupby(["state", "songid"])['max_gap_song_state'].shift(1)
units_of_observation['avg_gap_song_state'] = units_of_observation.groupby(["state", "songid"])['avg_gap_song_state'].shift(1)
units_of_observation['median_gap_song_state'] = units_of_observation.groupby(["state", "songid"])['median_gap_song_state'].shift(1)


units_of_observation['min_gap_song_state'] = units_of_observation.groupby(["state", "songid"])['min_gap_song_state'].fillna(method="ffill")
units_of_observation['max_gap_song_state'] = units_of_observation.groupby(["state", "songid"])['max_gap_song_state'].fillna(method="ffill")
units_of_observation['avg_gap_song_state'] = units_of_observation.groupby(["state", "songid"])['avg_gap_song_state'].fillna(method="ffill")
units_of_observation['median_gap_song_state'] = units_of_observation.groupby(["state", "songid"])['median_gap_song_state'].fillna(method="ffill")

units_of_observation['min_gap_song_state'] = units_of_observation['min_gap_song_state'].fillna(units_of_observation['time_last_played_song_state'])
units_of_observation['max_gap_song_state'] = units_of_observation['max_gap_song_state'].fillna(units_of_observation['time_last_played_song_state'])
units_of_observation['avg_gap_song_state'] = units_of_observation['avg_gap_song_state'].fillna(units_of_observation['time_last_played_song_state'])
units_of_observation['median_gap_song_state'] = units_of_observation['median_gap_song_state'].fillna(units_of_observation['time_last_played_song_state'])



units_of_observation = units_of_observation.merge(phish_sets.loc[:, ['showid', 'songid', 'num_times_played_song', 'num_times_played_song_venue', 'num_times_played_song_run', 'num_times_played_song_tour', 'num_times_played_song_state']], how="left", on=['showid', 'songid'])


units_of_observation = units_of_observation.sort_values(["songid", "showdate", "showid"]).reset_index(drop=True)



units_of_observation['num_times_played_song'] = units_of_observation.groupby(["songid"])['num_times_played_song'].shift(1)
units_of_observation['num_times_played_song'] = units_of_observation.groupby(["songid"])['num_times_played_song'].fillna(method="ffill")


units_of_observation = units_of_observation.sort_values(["venueid", "songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['num_times_played_song_venue'] = units_of_observation.groupby(["venueid","songid"])['num_times_played_song_venue'].shift(1)
units_of_observation['num_times_played_song_venue'] = units_of_observation.groupby(["venueid","songid"])['num_times_played_song_venue'].fillna(method="ffill")



units_of_observation = units_of_observation.sort_values(["runid", "songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['num_times_played_song_run'] = units_of_observation.groupby(["runid", "songid"])['num_times_played_song_run'].shift(1)
units_of_observation['num_times_played_song_run'] = units_of_observation.groupby(["runid", "songid"])['num_times_played_song_run'].fillna(method="ffill")

units_of_observation = units_of_observation.sort_values(["new_tourid", "songid", "showdate", "showid"]).reset_index(drop=True)

units_of_observation['num_times_played_song_tour'] = units_of_observation.groupby(["new_tourid", "songid"])['num_times_played_song_tour'].shift(1)
units_of_observation['num_times_played_song_tour'] = units_of_observation.groupby(["new_tourid", "songid"])['num_times_played_song_tour'].fillna(method="ffill")

units_of_observation = units_of_observation.sort_values(["state", "songid",  "showdate", "showid"]).reset_index(drop=True)

units_of_observation['num_times_played_song_state'] = units_of_observation.groupby(["state", "songid"])['num_times_played_song_state'].shift(1)
units_of_observation['num_times_played_song_state'] = units_of_observation.groupby(["state", "songid"])['num_times_played_song_state'].fillna(method="ffill")

## grab the song debut date and get time since the debut
units_of_observation['time_since_song_debut'] = units_of_observation['showdate'] - units_of_observation['song_debut_date']
units_of_observation['time_since_song_debut'] = units_of_observation['time_since_song_debut'].dt.days

## merge to ablums to get album numbers
units_of_observation = units_of_observation.merge(albums, how="left", on=["songid", "showid"])
units_of_observation = units_of_observation.sort_values(["showdate", "showid", "songid"]).reset_index(drop=True)

## merge over to see if song is a cover.
units_of_observation = units_of_observation.merge(covers, how="left", on=["songid"])

del covers, albums, phish_sets

units_of_observation = units_of_observation.sort_values(["showdate", "showid", "songid"]).reset_index(drop=True)


## need to really add in the min/max/avg gap of a song from last tour or year. That is not currently available.

## subset to get the new show data
new_show = units_of_observation.loc[units_of_observation["showdate"]==date_of_new_show, ['showid', 'songid','played', 
                                                    'time_last_played_song', 'time_last_played_song_venue',
                                                    'time_last_played_song_run', 'time_last_played_song_tour',
                                                    'time_last_played_song_state', 'min_gap_song', 'max_gap_song',
                                                    'avg_gap_song', 'median_gap_song', 'min_gap_song_venue',
                                                    'max_gap_song_venue', 'avg_gap_song_venue', 'median_gap_song_venue',
                                                    'min_gap_song_run', 'max_gap_song_run', 'avg_gap_song_run',
                                                    'median_gap_song_run', 'min_gap_song_tour', 'max_gap_song_tour',
                                                    'avg_gap_song_tour', 'median_gap_song_tour', 'min_gap_song_state',
                                                    'max_gap_song_state', 'avg_gap_song_state', 'median_gap_song_state',
                                                    'num_times_played_song', 'num_times_played_song_venue',
                                                    'num_times_played_song_run', 'num_times_played_song_tour',
                                                    'num_times_played_song_state', 'num_times_song_played_last_tour',
                                                    'min_num_times_song_played_tour', 'max_num_times_song_played_tour',
                                                    'avg_num_times_song_played_tour', 'median_num_times_song_played_tour',
                                                    'num_times_song_played_state_last_tour',
                                                    'min_num_times_song_played_state_tour',
                                                    'max_num_times_song_played_state_tour',
                                                    'avg_num_times_song_played_state_tour',
                                                    'median_num_times_song_played_state_tour',
                                                    'num_times_song_played_venue_last_tour',
                                                    'min_num_times_song_played_venue_tour',
                                                    'max_num_times_song_played_venue_tour',
                                                    'avg_num_times_song_played_venue_tour',
                                                    'median_num_times_song_played_venue_tour', 'rate_song_played_last_tour',
                                                    'min_rate_song_played_tour', 'max_rate_song_played_tour',
                                                    'avg_rate_song_played_tour', 'median_rate_song_played_tour',
                                                    'rate_song_played_state_last_tour', 'min_rate_song_played_state_tour',
                                                    'max_rate_song_played_state_tour', 'avg_rate_song_played_state_tour',
                                                    'median_rate_song_played_state_tour',
                                                    'rate_song_played_venue_last_tour', 'min_rate_song_played_venue_tour',
                                                    'max_rate_song_played_venue_tour', 'avg_rate_song_played_venue_tour',
                                                    'median_rate_song_played_venue_tour', 'time_since_song_debut',
                                                    'album_num',  'is_cover'
                                                    ]]


## subset to get all data
units_of_observation = units_of_observation.loc[units_of_observation["showdate"]<date_of_new_show, ['showid','songid','played', 
                                                    'time_last_played_song', 'time_last_played_song_venue',
                                                    'time_last_played_song_run', 'time_last_played_song_tour',
                                                    'time_last_played_song_state', 'min_gap_song', 'max_gap_song',
                                                    'avg_gap_song', 'median_gap_song', 'min_gap_song_venue',
                                                    'max_gap_song_venue', 'avg_gap_song_venue', 'median_gap_song_venue',
                                                    'min_gap_song_run', 'max_gap_song_run', 'avg_gap_song_run',
                                                    'median_gap_song_run', 'min_gap_song_tour', 'max_gap_song_tour',
                                                    'avg_gap_song_tour', 'median_gap_song_tour', 'min_gap_song_state',
                                                    'max_gap_song_state', 'avg_gap_song_state', 'median_gap_song_state',
                                                    'num_times_played_song', 'num_times_played_song_venue',
                                                    'num_times_played_song_run', 'num_times_played_song_tour',
                                                    'num_times_played_song_state', 'num_times_song_played_last_tour',
                                                    'min_num_times_song_played_tour', 'max_num_times_song_played_tour',
                                                    'avg_num_times_song_played_tour', 'median_num_times_song_played_tour',
                                                    'num_times_song_played_state_last_tour',
                                                    'min_num_times_song_played_state_tour',
                                                    'max_num_times_song_played_state_tour',
                                                    'avg_num_times_song_played_state_tour',
                                                    'median_num_times_song_played_state_tour',
                                                    'num_times_song_played_venue_last_tour',
                                                    'min_num_times_song_played_venue_tour',
                                                    'max_num_times_song_played_venue_tour',
                                                    'avg_num_times_song_played_venue_tour',
                                                    'median_num_times_song_played_venue_tour', 'rate_song_played_last_tour',
                                                    'min_rate_song_played_tour', 'max_rate_song_played_tour',
                                                    'avg_rate_song_played_tour', 'median_rate_song_played_tour',
                                                    'rate_song_played_state_last_tour', 'min_rate_song_played_state_tour',
                                                    'max_rate_song_played_state_tour', 'avg_rate_song_played_state_tour',
                                                    'median_rate_song_played_state_tour',
                                                    'rate_song_played_venue_last_tour', 'min_rate_song_played_venue_tour',
                                                    'max_rate_song_played_venue_tour', 'avg_rate_song_played_venue_tour',
                                                    'median_rate_song_played_venue_tour', 'time_since_song_debut',
                                                    'album_num',  'is_cover'
                                                    ]] 



## Only keep observations after the song has debuted
units_of_observation = units_of_observation.loc[units_of_observation["time_since_song_debut"]>0, :].reset_index(drop=True)
units_of_observation = units_of_observation.fillna(value=-1)



new_show = new_show.loc[new_show["time_since_song_debut"]>0, :].reset_index(drop=True)
new_show = new_show.fillna(value=-1)


## write out. 
units_of_observation.to_csv("set_charactersitics.csv", index=False)
new_show.to_csv("new_show_set_charactersitics.csv", index=False)


