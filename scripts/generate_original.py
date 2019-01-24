import requests
import json
import popelines
import os
from datetime import datetime

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


def generate_project_experiment(project_endpoint, experiment_endpoint):

    # get all projects
    try:
        response_proj = requests.get(project_endpoint, headers=headers)
        j_proj = json.loads(response_proj.text)
        response_proj.raise_for_status()
        print('Successfully read project data.')

    except requests.exceptions.HTTPError as err:
        print(err)

    
    # store a list of project metadata
    project_list = list()

    for project in j_proj:
        p_id = project['id']

        # get all experiments from one project
        params = (
            ('project_id', p_id),
            ('per_page', 100),
        ) 

        try:
            response_exp = requests.get(experiment_endpoint, headers=headers, params=params)
            j_exp = json.loads(response_exp.text)
            response_exp.raise_for_status()
            print('Successfully read experiment data.')
        except requests.exceptions.HTTPError as err:
            print(err)

        # loop for all experiments in this project
        upload_exp_list = []
        for exp in j_exp:
            exp['project_id'] = p_id
            upload_exp_list.append(flatten(exp, {}, ''))

        # upload experiment 
        pope.write_to_json(file_name='../uploads/experiments.json', jayson=upload_exp_list, mode='w')
        pope.write_to_bq(table_name='experiments', file_name='../uploads/experiments.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        print(f"Successfully uploaded experiments for project {p_id}")

    project_list.append(project)

    pope.write_to_json(file_name='../uploads/projects.json', jayson=project_list, mode='w')
    pope.write_to_bq(table_name='projects', file_name='../uploads/projects.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
    print("Successfully uploaded all projects.")



def generate_experiment_results(ts_endpoint, ):
    # get result time series from all experiments:
    for exp in j_exp:
        experiment_id = exp['id']
        response_ts = requests.get(f'https://api.optimizely.com/v2/experiments/{experiment_id}/timeseries', headers=headers)

        j_ts = json.loads(response_ts.text)

        # this function will be called in the fix_json_values and passed on to callback, to reset id keys to be a key:val pair ('id': {string of id})
        def fix_values(value, key):
            if key == 'results':
                new_list = []
                for x in value:
                    value[x]['result_id'] = x
                    new_list.append(value[x])
                return new_list
            else:
                return value
        
        new_j_ts = pope.fix_json_values(callback=fix_values, obj=j_ts)

        # with keys properly reset, we need to populate upper level into each row of each list level
        # Trying to flatten inner level 'timeseries'
        flattened_timeseries = []
        for element in new_j_ts['metrics'][0]['results'][0]['timeseries']:
            flattened_timeseries.append(flatten(element, {}, ''))

        # Replace old 'timeseries' with new 'flattened_timeseries'
        updated_results = populating_vals(outer_dict=new_j_ts['metrics'][0]['results'][0], inner_flattened_list=flattened_timeseries, destination_key='timeseries')
        flattened_results = flatten_dupe_vals(vals=updated_results, key='timeseries')

        # Replace old 'metrics' with new 'flattened_results'
        update_metrics = populating_vals(outer_dict=new_j_ts['metrics'][0], inner_flattened_list=flattened_results, destination_key='results')
        flattened_metrics = flatten_dupe_vals(vals=update_metrics, key='results')

        
        # upload results 
        pope.write_to_json(file_name='../uploads/result_ts.json', jayson=flattened_metrics, mode='w')
        # pope.write_to_bq(table_name='result_ts', file_name='../uploads/result_ts.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        # print(f"Successfully uploaded result time series for all experiments in experiment {experiment_id}.")


if __name__ == '__main__':

    gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

    pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)

    headers = {
        'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
    }

    ############################################### Project-Experiment #############################################
    p_endpoint = 'https://api.optimizely.com/v2/projects'
    e_endpoint = 'https://api.optimizely.com/v2/experiments'

    generate_project_experiment(project_endpoint=p_endpoint, experiment_endpoint=e_endpoint)


    ############################################### Experiment-Results #############################################
