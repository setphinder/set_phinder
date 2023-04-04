# -*- coding: utf-8 -*-
"""
Created on Sun Nov 13 14:37:15 2022

@author: Benjamin Walworth

This file pulls all of the prepared data together and subsets for modeling


"""

## import needed modules
import pandas as pd
import os
import json

## Set directory of configuration and read in.
project_dir = "C:\\Users\\benwa\\Documents\\projects\\set_phinder"
os.chdir(project_dir) 

## read in config
with open('config.json', 'r') as openfile:
    # Reading from json file
    config = json.load(openfile)


## pull in prepared data
os.chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["interim"])

setlist = pd.read_csv("set_charactersitics.csv")
show_characteristics= pd.read_csv("show_venue_characteristics.csv")
location_info = pd.read_csv("venues_geo.csv")
new_show = pd.read_csv("new_show_set_charactersitics.csv")



## Merge together
setlist = setlist.merge(show_characteristics.loc[:, ["showid", "new_tourid", 'showyear', "showmonth", "showday", "venueid", 'num_phish_show_venue',
                                                    'time_since_last_phish_venue', 'current_run_venue',
                                                    'num_phish_show_state', 'time_since_last_phish_state',
                                                    'time_since_last_phish_show', 'phish_show_number', 'days_left_on_tour',
                                                    'season', 'tour_season', 'days_on_tour', 'tour_show_number',
                                                    'day_of_week', 'time_since_formation']], how="left", on=["showid"])




setlist = setlist.merge(location_info.loc[:, ["venueid", "latitude", "longitude"]], how="left", on=["venueid"])

## Only keep those were we have a location of some sort. There were very few where we had nothing, so we are droping missing out.
setlist = setlist.loc[setlist["latitude"].notnull(), :].reset_index(drop=True)

## remove venue ID
setlist = setlist.drop(columns=["venueid"])


#############################################
## Do same for new shows.

new_show = new_show.merge(show_characteristics.loc[:, ["showid", "new_tourid", 'showyear', "showmonth", "showday", "venueid", 'num_phish_show_venue',
                                                    'time_since_last_phish_venue', 'current_run_venue',
                                                    'num_phish_show_state', 'time_since_last_phish_state',
                                                    'time_since_last_phish_show', 'phish_show_number', 'days_left_on_tour',
                                                    'season', 'tour_season', 'days_on_tour', 'tour_show_number',
                                                    'day_of_week', 'time_since_formation']], how="left", on=["showid"])




new_show = new_show.merge(location_info.loc[:, ["venueid", "latitude", "longitude"]], how="left", on=["venueid"])


new_show = new_show.loc[setlist["latitude"].notnull(), :].reset_index(drop=True)


new_show = new_show.drop(columns=["venueid"])



#################################

## drop any songs out that haven't been played at least 4 times total. This scews our population but also significantly reduces what could be extreme amounts of potential songs.

x = setlist.groupby(['songid'])["played"].sum().reset_index()
x = x.loc[x['played']>=4, :].reset_index(drop=True)



setlista = setlist.loc[setlist["songid"].isin(x['songid'].unique().tolist())==True, :].reset_index(drop=True)
new_show_removed_songs_f = new_show.loc[new_show['songid'].isin(x['songid'].unique().tolist())==True, :].reset_index(drop=True)




## Export analytical sets for prediction and modeling.
os.chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["final"])
setlista.to_csv("analytical_set_full.csv", index=False)
new_show_removed_songs_f.to_csv("new_show_analytical_set_full.csv", index=False)

if config['trn_params']['refit_type'] == "refit":
    os.chdir(config["dirs"]["rootdir"]+config["dirs"]["model"]["root_mdl"]+config["dirs"]["model"]["trn_data"])
    setlista.to_csv(config['trn_params']['trn_data_name'], index=False)

    
