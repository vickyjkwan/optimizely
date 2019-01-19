import requests
import json
import popelines
import os
from datetime import datetime


# a function to detect deeply nested json file, with a mix of dictionaries and lists
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


if __name__ == "main()":

    gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

    pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)

    headers = {
        'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
    }

    # Upload Project-Experiment table
    # get all projects
    try:
        response_proj = requests.get('https://api.optimizely.com/v2/projects', headers=headers)
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
            response_exp = requests.get('https://api.optimizely.com/v2/experiments', headers=headers, params=params)
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
        pope.write_to_json(file_name='experiments.json', jayson=upload_exp_list, mode='w')
        pope.write_to_bq(table_name='experiments', file_name='experiments.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)


        project_list.append(project)

    pope.write_to_json(file_name='projects.json', jayson=project_list, mode='w')
    pope.write_to_bq(table_name='projects', file_name='projects.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
