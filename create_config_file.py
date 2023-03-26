# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 11:46:04 2023

@author: Benjamin Walworth

This file is used to create the configuration file parameters to update and predict each date.
"""

## Create json config
import json
import os

## where you have your api key stored
api_dir = r"C:\\Users\\benwa\\Documents\\projects"
project_dir = "C:\\Users\\benwa\\Documents\\projects\\set_phinder"

## set the api directory
os.chdir(api_dir)

## This is where you store the API key and read this in. This should be secure.
with open("api_key_phishnet.txt") as f:
    apikey = f.readlines()
apikey = apikey[0]

## params and directory are set up to handle data.
config = {"phish_api_key":apikey,
          "dirs":
              {"rootdir": "C:\\Users\\benwa\\Documents\\projects\\set_phinder\\",
               "dataprep": {"root_dp": "data_prep\\data\\",
                            "raw": "raw",
                            "interim":"interim",
                            "final":"final"},
               
               "model": {"root_mdl": "model\\data\\",
                         "mdl_objects": "mdl_objects",
                         "trn_data": "trn_data",
                         "prd_results": "prd_results",
                         "viz": "viz",
                         "finl_out": "finl_out",
                         "mdl_summary":"mdl_summary"}
                         
                   },
              
              "trn_params":
                  {"random_seed":1994,
                   "group_var": "showyear",
                   "outcom_var": "played",
                   "group_var_start":1986,
                   "trn_data_name": "analytical_set_full_trn.csv",
                   "train_val_ss": 2018,
                   "extra_validation":2020,
                   "gap_size_for_tv": 7
                   }
                  }
              

os.chdir(project_dir) 
with open("config.json", "w") as outfile:
    json.dump(config, outfile)


    
    
    