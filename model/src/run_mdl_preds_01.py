# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 14:53:08 2023

@author: Benjamin Walworth

This file uses a fitted model object to predict probabilities of songs for upcomming shows.

"""

## Import needed modules
from os import chdir
import pandas as pd
import joblib
import numpy as np
import copy
import json

## Preprocessing
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import  RandomOverSampler 



## Models
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import BernoulliNB
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis
from xgboost import XGBClassifier
from sklearn.neighbors import KNeighborsClassifier


## multi-processing
import multiprocessing as mp


## Define a few helper functions

def get_best_param_lists(md, params_ck):
    """
    

    Parameters
    ----------
    md : TYPE: string
        DESCRIPTION: model name of classifier.
    params_ck : TYPE: dictionary
        DESCRIPTION. a evaluated list of parameters from the best classifier

    Returns
    -------
    new_params : TYPE: dictionary
        DESCRIPTION. a dictionary of model object parameters to fit a classifier
    pre_processing_steps : TYPE dictionary
        DESCRIPTION. a dictionary of preprocessing steps for data to be fed to the classifier

    """
    

    new_params = {k.replace(str(md)+"__", ""): params_ck[k] for k in list(params_ck.keys()) if str(md)+"__" in k}
    pre_processing_steps = {v: params_ck[v] for v in list(params_ck.keys()) if v in ["fs", "scaler", "sampler"]}
    
    return new_params, pre_processing_steps


def get_unfit_mdl_obj(md, new_params, nsl=-1, rs=1994):
    """
    

    Parameters
    ----------
    md : TYPE: string
        DESCRIPTION. model object name
    new_params : TYPE dictionary
        DESCRIPTION. model object parameters
    nsl : TYPE, optional integer
        DESCRIPTION. The default is -1. number of cores to run on
    rs : TYPE, optional integer or random state generator
        DESCRIPTION. The default is 1994. this is a random state to pass to model objects

    Returns
    -------
    main_mdl_obj : TYPE model object classifier
        DESCRIPTION. an unfit model object classifier with set paprameters for fitting based on given data.

    """
    
    mdl_objs = {'rf':RandomForestClassifier(random_state=rs), 'xgbl':XGBClassifier(scale_pos_weight=1, random_state=rs, use_label_encoder=False), 'gbf':GradientBoostingClassifier(random_state=rs), 'ada':AdaBoostClassifier(random_state=rs), 'bb':BernoulliNB(), 'gb':GaussianNB(), 'qda':QuadraticDiscriminantAnalysis(), 'lda':LinearDiscriminantAnalysis(), 'lda_shrink':LinearDiscriminantAnalysis(solver= "lsqr", shrinkage="auto"), 'knn':KNeighborsClassifier()}
    
    main_mdl_obj = mdl_objs[md]
    main_mdl_obj.set_params(**new_params)
    if 'n_jobs' in main_mdl_obj.get_params().keys():
        main_mdl_obj.set_params(**{'n_jobs':nsl})
                  
    
    if 'random_state' in main_mdl_obj.get_params().keys():
        main_mdl_obj.set_params(**{'random_state':rs})        
        
        
    if 'random_state' in main_mdl_obj.get_params().keys():
        main_mdl_obj.set_params(**{'random_state':rs})
        
    return main_mdl_obj

def preprocess_for_retrain(pre_processing_steps, train_x_m, train_y_m, test_x_m=None):
    
    if pre_processing_steps["scaler"] is None:

        
        ref_train_data_v1 = copy.deepcopy(train_x_m)
        if test_x_m is None:
            pass
        else:
            test_x_new = copy.deepcopy(test_x_m)   
    
    else:
        scaler_a = copy.deepcopy(pre_processing_steps["scaler"])
        
        ## Needed for non_refit
        ref_train_data_v1 = scaler_a.fit_transform(train_x_m)
        if test_x_m is None:
            pass
        else:
            test_x_new = scaler_a.transform(test_x_m)
       
        
        del scaler_a

  
        
    ## fs
    if pre_processing_steps["fs"] is None:
        pass
    else:    
        scaler_a = copy.deepcopy(pre_processing_steps["fs"])

        
        ref_train_data_v1 = scaler_a.fit_transform(ref_train_data_v1, train_y_m)
        if test_x_m is None:
            pass
        else:
            test_x_new = copy.deepcopy(test_x_new[:, scaler_a.get_support()])
        
        del scaler_a

  

    ## fs
    if pre_processing_steps["sampler"] is None:
        ref_train_y_v1 = copy.deepcopy(train_y_m)        
    else:
        scaler_a = copy.deepcopy(pre_processing_steps["sampler"])
  
        
        ref_train_data_v1, ref_train_y_v1  = scaler_a.fit_resample(ref_train_data_v1, train_y_m)
        
        
        del scaler_a


        
        
    if test_x_m is None:
        return ref_train_data_v1, ref_train_y_v1
    else:        
        return ref_train_data_v1, ref_train_y_v1, test_x_new

    
    
def get_features_selected(pre_processing_steps, train_x_m, train_y_m):
    
    if pre_processing_steps["scaler"] is None:
        ref_train_data_v1 = copy.deepcopy(train_x_m)
    else:
        
        scaler_a = copy.deepcopy(pre_processing_steps["scaler"])
        
        ## Needed for non_refit
        ref_train_data_v1 = scaler_a.fit_transform(train_x_m)
       
        
        del scaler_a


  
        
    ## fs
    if pre_processing_steps["fs"] is None:
        indicies_rtn = np.repeat(np.array([True]), ref_train_data_v1.shape[1])
        
    else:
        scaler_a = copy.deepcopy(pre_processing_steps["fs"])

        
        ref_train_data_v1 = scaler_a.fit_transform(ref_train_data_v1, train_y_m)

        indicies_rtn = scaler_a.get_support()
        

        
    return indicies_rtn
        

        
def refit_mdl_obj(main_mdl_obj, ref_train_data_v1, ref_train_y_v1):
    
    mdl_obj_w_fit = copy.deepcopy(main_mdl_obj)
    
    mdl_obj_w_fit.fit(ref_train_data_v1, ref_train_y_v1)
    
    return mdl_obj_w_fit



## Get configuration file
project_dir = "C:\\Users\\benwa\\Documents\\projects\\set_phinder"
chdir(project_dir) 

## read in config
with open('config.json', 'r') as openfile:
    # Reading from json file
    config = json.load(openfile)


nsl = int(mp.cpu_count()-1)

## Get model parameters data
chdir(config["dirs"]["rootdir"]+config["dirs"]["model"]["root_mdl"]+config["dirs"]["model"]["mdl_summary"])
selected_models = pd.read_csv("selected_mdl_params.csv")

        
## Get newest analytical data
chdir(config["dirs"]["rootdir"]+config["dirs"]["dataprep"]["root_dp"]+config["dirs"]["dataprep"]["final"])
new_data_to_check = pd.read_csv("new_show_analytical_set_full.csv")

for col in new_data_to_check:
    if new_data_to_check[col].dtype == "int64":
        new_data_to_check[col] = new_data_to_check[col].astype(float)

del col


## Set some model data params
group_var = config['trn_params']['group_var']
outcom_var = config['trn_params']['outcom_var']

##convert to all float, unless outcome variable
modeldata = new_data_to_check.copy(deep=True)
for col in modeldata:
    if modeldata[col].dtype == "int64":
        modeldata[col] = modeldata[col].astype(float)
    if col == outcom_var:
        modeldata[col] = modeldata[col].astype(np.int64)

## Subset to only keep those past the starting time and drop out uneeded columns
modeldata = modeldata.loc[modeldata[group_var]>=config['trn_params']['group_var_start'], :].reset_index(drop=True)
modeldata = modeldata.sort_values(['time_since_formation', 'new_tourid', 'songid', 'showid']).reset_index(drop=True)

## make it so we can match to songs and shows
shows_to_match = modeldata.loc[:, ["songid","showid"]]
modeldata = modeldata.drop(columns=['showid', 'songid', 'new_tourid'])


## get columns
train_cols = list(range(len(modeldata.columns)))

## Need to figure out how to get this programatically
outcom_var_index = modeldata.columns.get_loc(outcom_var)
    
train_cols.remove(outcom_var_index)

group_var_index = modeldata.columns.get_loc(group_var)
train_cols.remove(group_var_index)

test_cols = outcom_var_index

del outcom_var_index


X_test = modeldata.iloc[:, train_cols].values


del  outcom_var, group_var, col, group_var_index, modeldata, new_data_to_check, test_cols, train_cols

## Get pre-processing parameters
df = selected_models.copy(deep=True)
algonm = df["model"][0]

params_cka = eval(df.loc[df.index==0, :]['params'].item())
new_params, pre_processing_steps =get_best_param_lists(algonm, params_cka)

del params_cka, new_params, algonm


## Load in model object
mdl = joblib.load(config["dirs"]["rootdir"]+config["dirs"]["model"]["root_mdl"]+config["dirs"]["model"]["mdl_objects"]+"\\"+"final_mdl_obj.sav")

del df

#######################################

## prep training data used to fit model
mod_data_pth = config['trn_params']['trn_data_name']
train_val_ss = config['trn_params']['train_val_ss']
extra_validation =  config['trn_params']['extra_validation']
group_var = config['trn_params']['group_var']
outcom_var = config['trn_params']['outcom_var']



chdir(config["dirs"]["rootdir"]+config["dirs"]["model"]["root_mdl"]+config["dirs"]["model"]["trn_data"])
modeldata = pd.read_csv(mod_data_pth)
for col in modeldata:
    if modeldata[col].dtype == "int64":
        modeldata[col] = modeldata[col].astype(float)
    if col == outcom_var:
        modeldata[col] = modeldata[col].astype(np.int64)
        
del col

modeldata = modeldata.loc[modeldata[group_var]>=config['trn_params']['group_var_start'], :].reset_index(drop=True)
modeldata = modeldata.sort_values(['time_since_formation', 'new_tourid', 'songid', 'showid']).reset_index(drop=True)


modeldata = modeldata.drop(columns=['showid', 'songid', 'new_tourid'])
        
train = modeldata.loc[modeldata[group_var]< train_val_ss, :].reset_index(drop=True)


del modeldata

## get columns
train_cols = list(range(len(train.columns)))

## Need to figure out how to get this programatically
outcom_var_index = train.columns.get_loc(outcom_var)


train_cols.remove(outcom_var_index)
group_var_index = train.columns.get_loc(group_var)
train_cols.remove(group_var_index)

test_cols = outcom_var_index

del outcom_var_index, group_var_index

X_train = train.iloc[:, train_cols].values
y_train = train.iloc[:, test_cols].values



del  train_cols, test_cols, extra_validation, group_var, mod_data_pth,  outcom_var, train_val_ss, selected_models, train

## Run pre-processing steps used on training data to pre-process new data.
df_X_t, df_y_t, X_test_p = preprocess_for_retrain(pre_processing_steps, train_x_m=X_train, train_y_m=y_train, test_x_m=X_test)
        

del df_X_t, df_y_t, X_train, y_train, X_test, pre_processing_steps

## using model object, predict on the new observations and store probabilities with song and show ids
y_pred_proba = mdl.predict_proba(X_test_p)

df_pred = pd.DataFrame({'y_pred_proba': y_pred_proba[:, 1]})

fin_out = pd.concat([shows_to_match, df_pred], axis=1)

del y_pred_proba, df_pred

## export predictions
chdir(config["dirs"]["rootdir"]+config["dirs"]["model"]["root_mdl"]+config["dirs"]["model"]["prd_results"])
fin_out.to_csv("preds_full.csv", index=False)











