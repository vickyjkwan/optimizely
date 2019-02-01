import requests
import json
import os
from datetime import datetime


# a function to detect deeply nested json file, with a mix of dictionaries and lists
# return a dictionary with one layer, and all keys only contains '_' and no '-'
def flatten(jayson, acc, prefix):
    if isinstance(jayson, dict):
        for k,v in jayson.items():
            if prefix:
                prefix_k = prefix + "_" + k
            else: 
                prefix_k = k
            prefix_k = prefix_k.replace('-', '_')
            
            if isinstance(v, dict):
                flatten(v, acc, prefix_k)
            elif isinstance(v, list):
                for j in v:
                    flatten(j, acc, prefix_k)
            else:
                acc[prefix_k] = v
        return acc 
    else:
        return acc


########################### functions not in use #######################################
def fix_values(value, key, reset_key):
    if key == reset_key:
        new_list = []
        for x in value:
            value[x][f'{reset_key}_id'] = x
            new_list.append(value[x])
        return new_list
    else:
        return value


# a function to populate upper level key:val pairs into lower level list of dictionaries
# returns a dictionary with the each row inherets all metadata from upper level
def populating_vals(outer_dict, inner_flattened_list, destination_key):
    # outer_dict is the upper level dict
    # inner_flattened_list is the lower level list of dicts that are each flattened
    # destination_key is the primary key of the lower level
    updated_dict = {}
    for k, v in outer_dict.items():
        if k != destination_key:
            updated_dict[k] = v
        else:
            updated_dict[k] = inner_flattened_list
    return updated_dict


# a function that first duplicates outer level key:val pairs, then flattens the duped list
# returns a list of flattened rows
def flatten_dupe_vals(vals, key):
    
    duped_results = []
    for element in vals[key]:
        ts_dict = populating_vals(outer_dict=vals, inner_flattened_list=element, destination_key=key)
        duped_results.append(ts_dict)

    flattened_results = []
    for element in duped_results:
        flattened_results.append(flatten(element, {}, ''))
        
    return flattened_results