# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 16:15:28 2023

@author: Benjamin Walworth

This file links song probabilities to the human readable format (ie song names vs ids), and only selects the top probabilities.
 
"""

## Import modules
import pandas as pd
import os
import json

## read in configuration file
project_dir = "C:\\Users\\benwa\\Documents\\projects\\set_phinder"
os.chdir(project_dir) 

## read in config
with open('config.json', 'r') as openfile:
    # Reading from json file
    config = json.load(openfile)


os.chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["raw"])


## Read in Shows
shows = pd.read_csv("shows.csv")

## Read in All Songs
songs = pd.read_csv("songs.csv")



## Convert todate
shows["showdate"] = pd.to_datetime(shows['showdate'])

## Sort and subset to phish shows
shows = shows.sort_values(by=["venueid", "showdate", "showid"]).reset_index(drop=True)


## read in predictions
os.chdir(config["dirs"]["rootdir"]+config["dirs"]["model"]["root_mdl"]+config["dirs"]["model"]["prd_results"])
res = pd.read_csv("preds_full.csv")

## merge up metadata for songs and shows, and convert to a string date
resa = res.merge(songs.loc[:, ["songid", "song"]].drop_duplicates(["songid"]).reset_index(drop=True), how="left", on="songid")
resa = resa.merge(shows.loc[:, ["showid", "venue", "city", "state", "country", "showdate"]].drop_duplicates(["showid"]).reset_index(drop=True), how="left", on=["showid"])
resa["showdate"] = resa["showdate"].apply(lambda x: x.strftime('%m/%d/%Y'))

## Only keep probabilities over 50%
resa = resa.loc[:, ["showid", "songid", "venue", "city", "state", "country", "showdate", "song", "y_pred_proba"]].sort_values(["y_pred_proba"], ascending=False).reset_index(drop=True)
resa = resa.loc[resa["y_pred_proba"]>.5, :].reset_index(drop=True)
resa = resa.rename(columns={"y_pred_proba":"probability"})

## export final predictions
os.chdir(config["dirs"]["rootdir"]+config["dirs"]["model"]["root_mdl"]+config["dirs"]["model"]["finl_out"])
resa.to_csv("predictions_for_next_show.csv", index=False)
