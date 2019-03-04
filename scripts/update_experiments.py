import json
import requests
import popelines
import os
from datetime import datetime, timedelta
from main import fix_values, populating_vals, flatten, flatten_dupe_vals
from generate_original_with_timeseries import read_endpoint, generate_projects, generate_experiments


if __name__ == '__main__':
    if not os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS'):
        os.environ['GOOGLE_ACCOUNT_CREDENTIALS'] = '/home/engineering/keyfile.json'
    gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

    directory = str(os.path.abspath(os.path.dirname(__file__)))
    # directory = os.getcwd()

    pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)
    headers = {
        'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
    }

    experiment_endpoint = 'https://api.optimizely.com/v2/experiments'

    ######################### updating experiments #########################
    experiment_query = open(f'{directory}/existing_experiments.sql').read()
    existing_experiments = []
    for result in pope.bq_query(experiment_query):
        existing_experiments.append((result[0], datetime.strftime(result[2], '%Y-%m-%dT%H:%M:%S.%fz')))

    project_query = open(f'{directory}/existing_projects.sql').read()
    existing_projects = []
    for result in pope.bq_query(project_query):
        existing_projects.append(result[0])    

    updated_experiments = []
    for project_id in existing_projects:
        # params include project_id (required) and experiments pulling per each request (default only 25)
        params = (
            ('project_id', project_id),
            ('per_page', 100),
        ) 

        exp_response = read_endpoint(endpoint=experiment_endpoint, headers_set=headers, params_set=params)
        exp_list = json.loads(exp_response.text)

        for exp in exp_list:
            # checking if exp is not in the existing experiment list, then append
            if exp['id'] not in [x[0] for x in existing_experiments]:
                updated_experiments.append(exp)

            # if exp is in the existing experiment list, then check if last modified timestamp was larger than last upload timestamp
            else:
                if exp['last_modified'] > [x[1] for x in existing_experiments if x[0] == exp['id']][0]:
                    updated_experiments.append(exp)
                    
                else:
                    None
        
    all_singles, metrics_table, variations_table = generate_experiments(updated_experiments)    

    pope.write_to_json(file_name=f'{directory}/update_experiments_single_fields.json', jayson=all_singles, mode='w')
    pope.write_to_bq(table_name='experiments_single_fields', file_name=f'{directory}/update_experiments_single_fields.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)

    pope.write_to_json(file_name=f'{directory}/update_experiments_metrics_table.json', jayson=metrics_table, mode='w')
    pope.write_to_bq(table_name='experiments_metrics_table', file_name=f'{directory}/update_experiments_metrics_table.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
    
    pope.write_to_json(file_name=f'{directory}/update_experiments_variations_table.json', jayson=variations_table, mode='w')
    pope.write_to_bq(table_name='experiments_variations_table', file_name=f'{directory}/update_experiments_variations_table.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
