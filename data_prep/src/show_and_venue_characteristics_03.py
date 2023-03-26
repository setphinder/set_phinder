# -*- coding: utf-8 -*-
"""
Created on Wed Oct  5 13:18:34 2022

@author: Benjamin Walworth

This file gets the all of the characteristics for any given show and venue (or city) that might be predictive.
To derive these calculations, we use all shows, not just those with full set lists.

"""

## import needed modules
import pandas as pd
import numpy as np
import os
import json


## set directory with config
project_dir = "C:\\Users\\benwa\\Documents\\projects\\set_phinder"
os.chdir(project_dir) 

## read in config
with open('config.json', 'r') as openfile:
    # Reading from json file
    config = json.load(openfile)

## Read in shows
os.chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["raw"])
shows = pd.read_csv("shows.csv")


## get just those that are Phish shows specifically
shows["showdate"] = pd.to_datetime(shows['showdate'])
shows = shows.sort_values(by=["venueid", "showdate", "showid"]).reset_index(drop=True)
phish_shows = shows.loc[shows["artistid"]==1, :].reset_index(drop=True)

## drop any missing from stats (ie do not count)
phish_shows = phish_shows.loc[phish_shows["exclude_from_stats"]==0, :].reset_index(drop=True)

## Get number of shows at venue, and time since last show at venue WHEN played
phish_shows["num_phish_show_venue"] = phish_shows.groupby(by=["venueid"]).cumcount()
phish_shows["last_show_date_venue"] = phish_shows.groupby(by=["venueid"])["showdate"].shift(1)
phish_shows["time_since_last_phish_venue"] = phish_shows["showdate"] - phish_shows["last_show_date_venue"]
phish_shows["time_since_last_phish_venue"] = phish_shows["time_since_last_phish_venue"].dt.days

## Get time till next show at venue. We assume we know this when we are making a given prediction, as we are predicting daily.
phish_shows["next_show_date_venue"] = phish_shows.groupby(by=["venueid"])["showdate"].shift(-1)
phish_shows["time_to_next_phish_venue"] = phish_shows["showdate"] - phish_shows["next_show_date_venue"]
phish_shows["time_to_next_phish_venue"] = phish_shows["time_to_next_phish_venue"].dt.days


phish_shows = phish_shows.sort_values(by=["showdate"]).reset_index(drop=True)

## here we derive those which are runs, but "non consecutive" This is like a bakers dozen when there are breaks in a run. 
non_consecutive_runs = phish_shows.loc[(phish_shows["time_since_last_phish_venue"].isnull()) | (phish_shows["time_since_last_phish_venue"]>2), :].reset_index(drop=True)
non_consecutive_runs = non_consecutive_runs.sort_values(["showdate", "venueid"]).reset_index(drop=True)
non_consecutive_runs["runid"] = non_consecutive_runs.index

## get id
phish_shows = phish_shows.merge(non_consecutive_runs.loc[:, ["showid", "runid"]], how="left", on="showid")
phish_shows = phish_shows.sort_values(by=["showdate", "showid"]).reset_index(drop=True)

## fill the run id
phish_shows["runid"] = phish_shows["runid"].fillna(method="ffill") 
phish_shows["current_run_venue"] = phish_shows.groupby(by=["runid"]).cumcount()

## give the "state" the country if missing
phish_shows["state"] = np.where(phish_shows["state"].isnull(), phish_shows["country"], phish_shows["state"])
phish_shows = phish_shows.sort_values(by=["state", "showdate", "showid", "venueid"]).reset_index(drop=True)

## get counts by state
phish_shows["num_phish_show_state"] = phish_shows.groupby(by=["state"]).cumcount()
phish_shows["last_show_date_state"] = phish_shows.groupby(by=["state"])["showdate"].shift(1)
phish_shows["time_since_last_phish_state"] = phish_shows["showdate"] - phish_shows["last_show_date_state"]
phish_shows["time_since_last_phish_state"] = phish_shows["time_since_last_phish_state"].dt.days

## get overall times sine and to next show
phish_shows = phish_shows.sort_values(by=["showdate", "showid"]).reset_index(drop=True)
phish_shows["prior_phish_show_date"] = phish_shows["showdate"].shift(1)
phish_shows["time_since_last_phish_show"] = phish_shows["showdate"] - phish_shows["prior_phish_show_date"]
phish_shows["time_since_last_phish_show"] = phish_shows["time_since_last_phish_show"].dt.days
phish_shows["phish_show_number"] = phish_shows.index
phish_shows["phish_show_number"] = phish_shows["phish_show_number"]+1


## Subset to tours and nontours
phish_tours = phish_shows.loc[(phish_shows["tourid"]!=61) & (phish_shows["tourid"].notnull()), :].reset_index(drop=True)
non_tours = phish_shows.loc[phish_shows["showid"].isin(phish_tours["showid"].unique().tolist())==False, :].reset_index(drop=True)

## subset to first day of tour
phish_tours = phish_tours.sort_values(by=["tourid", "showdate", "showid"]).reset_index(drop=True)
phish_tours_ss = phish_tours.drop_duplicates(["tourid"]).reset_index(drop=True)

## subset to first day of run
non_tours = non_tours.sort_values(by=["runid", "showdate", "showid"]).reset_index(drop=True)
non_tours_ss = non_tours.drop_duplicates(["runid"]).reset_index(drop=True)

## concat and sort by first day
tour_id = pd.concat([phish_tours_ss, non_tours_ss ], axis=0, join="outer", sort=False, ignore_index=True)

tour_id = tour_id.sort_values(by=["showdate", "showid"]).reset_index(drop=True)

## create new id, for tour or run but all in one
tour_id = tour_id.reset_index(drop=False).rename(columns={"index":"new_tourid"})

## merge tours back to tours and runs back to runs
phish_tours = phish_tours.merge(tour_id.loc[:, ["tourid", "new_tourid"]], how="left", on="tourid")
non_tours = non_tours.merge(tour_id.loc[:, ["runid", "new_tourid"]], how="left", on="runid")

## reconcat
phish_tours = pd.concat([phish_tours, non_tours], axis=0, join="outer", sort=True, ignore_index=True)

## gives us new tour id
phish_shows = phish_shows.merge(phish_tours.loc[:, ["showid", "new_tourid"]], how="left", on="showid")

phish_tours = phish_shows.loc[phish_shows["new_tourid"].notnull(), :].reset_index(drop=True)
phish_tours = phish_tours.sort_values(["new_tourid", "showdate"]).reset_index(drop=True)
phish_tour_first_day = phish_tours.drop_duplicates(["new_tourid"]).reset_index(drop=True)
phish_tour_first_day["tour_first_date"] = phish_tour_first_day["showdate"].copy()
phish_tour_first_day = phish_tour_first_day.loc[:, ["new_tourid", "tour_first_date"]]



## creating a variable called "days left on tour" think we can get away with it because tour dates are set ahead of time and are known usually on the day of a show.
## we assume the last day of the tour is known as of the date
phish_tour_last_day = phish_tours.drop_duplicates(["new_tourid"], keep="last").reset_index(drop=True)

phish_tour_last_day["tour_last_date"] = phish_tour_last_day["showdate"].copy()
phish_tour_last_day = phish_tour_last_day.loc[:, ["new_tourid", "tour_last_date"]]



phish_shows = phish_shows.merge(phish_tour_last_day, how="left", on="new_tourid")
phish_shows["days_left_on_tour"] = phish_shows["showdate"] - phish_shows["tour_last_date"]
phish_shows["days_left_on_tour"] = phish_shows["days_left_on_tour"].dt.days

## derive a tour season, with nye being its own thing
date = phish_tour_first_day.tour_first_date.dt.month*100 +  phish_tour_first_day.tour_first_date.dt.day
phish_tour_first_day["tour_season"] = (pd.cut(date,[0,105,321,620,922,1220,1226,1300],
                       labels=['nye_run','winter','spring','summer','autumn','winter ', 'nye_run '])
                  .str.strip()
               )

## get days been on tour
phish_shows = phish_shows.merge(phish_tour_first_day, how="left", on="new_tourid")
phish_shows["days_on_tour"] = phish_shows["showdate"] - phish_shows["tour_first_date"]
phish_shows["days_on_tour"] = phish_shows["days_on_tour"].dt.days


phish_tours = phish_tours.sort_values(["tourid", "showdate"]).reset_index(drop=True)

## get number of show for tour
phish_tours["tour_show_number"] = phish_tours.groupby(["tourid"]).cumcount()
phish_tours["tour_show_number"] = phish_tours["tour_show_number"]+1

phish_shows = phish_shows.merge(phish_tours.loc[:, ["showid", "tour_show_number"]], how="left", on="showid")

## get seaon in general
date = phish_shows.showdate.dt.month*100 +  phish_shows.showdate.dt.day
phish_shows["season"] = (pd.cut(date,[0,105,321,620,922,1220,1226,1300],
                       labels=['nye_run','winter','spring','summer','autumn','winter ', 'nye_run '])
                  .str.strip()
               )

## give regular seaon if tour seasonmissing
phish_shows["tour_season"] = np.where(phish_shows["tour_season"].notnull(), phish_shows["tour_season"], phish_shows["season"])

## get day of week
phish_shows["day_of_week"] = phish_shows["showdate"].dt.day_name()



########################################################################################################

phish_shows = phish_shows.loc[:, ['showid', 'showyear', 'showmonth', 'showday', 'showdate', 'venueid', 'tourid', 'new_tourid','runid',
                                  'num_phish_show_venue', 'time_since_last_phish_venue', 'current_run_venue', 
                                  'num_phish_show_state', 'time_since_last_phish_state', 'time_since_last_phish_show', 
                                  'phish_show_number', 
                                  'days_left_on_tour', 'season', 'tour_season', 'days_on_tour', 'tour_show_number',
                                  'day_of_week'
                                  ]]

## get the times since last show at the venue, state and just in general
phish_shows['time_since_last_phish_venue'] = phish_shows['time_since_last_phish_venue'].fillna(value=-1)
phish_shows['time_since_last_phish_state'] = phish_shows['time_since_last_phish_state'].fillna(value=-1)
phish_shows['time_since_last_phish_show'] = phish_shows['time_since_last_phish_show'].fillna(value=-1)

## Get the number of days left on tour. This is not doable if doing a run, because we likely won't know when that ends, but could. It depends.
phish_shows['days_left_on_tour'] = phish_shows['days_left_on_tour'].abs()
phish_shows['days_left_on_tour'] = phish_shows['days_left_on_tour'].fillna(value=-1)

## get number of days on tour, if not on tour, then we will use the run count
phish_shows['days_on_tour'] = np.where(phish_shows['days_on_tour'].notnull(), phish_shows['days_on_tour'], phish_shows['current_run_venue'])

## Get the show number of the tour, if not use the run count
phish_shows['tour_show_number'] = np.where(phish_shows['tour_show_number'].notnull(), phish_shows['tour_show_number'], phish_shows['current_run_venue'])

## Map these to numeric
season_map = {'nye_run':0,'winter':1,'spring':2,'summer':3,'autumn':4}
day_map = {'Monday':0, 'Tuesday':1, 'Wednesday':2, 'Thursday':3, 'Friday':4, 'Saturday':5, 'Sunday':6}

phish_shows['season'] = phish_shows['season'].map(season_map).fillna(phish_shows['season'])
phish_shows['tour_season'] = phish_shows['tour_season'].map(season_map).fillna(phish_shows['tour_season'])

phish_shows['day_of_week'] = phish_shows['day_of_week'].map(day_map).fillna(phish_shows['day_of_week'])

## Get time since formation
phish_shows['time_since_formation'] = phish_shows["showdate"] - pd.to_datetime("1983-10-01")
phish_shows['time_since_formation'] = phish_shows['time_since_formation'].dt.days

## Export the show/venue/tour/state characteristics.
os.chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["interim"])
phish_shows.to_csv("show_venue_characteristics.csv", index=False)
