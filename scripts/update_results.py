import json
import requests
import popelines
import os
from datetime import datetime, timedelta
from main import fix_values, populating_vals, flatten, flatten_dupe_vals
from generate_original_with_timeseries import read_endpoint, generate_results
from generate_original_results import results_generator

# ############################################### Keys and Authentication #######################################
if not os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS'):
    os.environ['GOOGLE_ACCOUNT_CREDENTIALS'] = '/home/engineering/keyfile.json'
gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

directory = str(os.path.abspath(os.path.dirname(__file__)))
# directory = os.getcwd()

############################################### Instantiating Popelines #######################################
pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)

# Optimizely parameters
headers = {
    'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
}

params = (
    ('per_page', 100),
)

# from bq get all running experiments
query = open(f'{directory}/running_experiments.sql').read()
running_exp = pope.bq_query(query)


# exp = running_exp[0]
new_results = []
for exp in running_exp:
    query = open(f'{directory}/last_results.sql').read().replace('exp_id', f"{exp['id']}")
    new_exp = pope.bq_query(query)
    
    if new_exp != []:
        start_timestamp = datetime.strftime(new_exp[0][1], '%Y-%m-%dT%H:%M:%SZ')[:14] + str('00:00Z')
        end_timestamp = datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%SZ')
        
        new_results.extend(results_generator(start_timestamp, end_timestamp, experiment_id=new_exp[0][0], headers=headers, params=params, pope=pope, gbq_key=gbq_key))
        pope.write_to_json(file_name=f'{directory}/updating_results.json', jayson=new_results, mode='w')

    else:
        starting_query = open(f'{directory}/new_results.sql').read().replace('exp_id', f"{exp['id']}")
        new_exp = pope.bq_query(starting_query)

        start_timestamp = datetime.strftime(new_exp[0][1], '%Y-%m-%dT%H:%M:%SZ')[:14] + str('00:00Z')
        end_timestamp = datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%SZ')
        
        new_results.extend(results_generator(start_timestamp, end_timestamp, experiment_id=new_exp[0][0], headers=headers, params=params, pope=pope, gbq_key=gbq_key))
        pope.write_to_json(file_name=f'{directory}/updating_results.json', jayson=new_results, mode='w')


pope.write_to_bq(table_name='results', file_name=f'{directory}/updating_results.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)    


