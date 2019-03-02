import json
import requests
import popelines
import os
from datetime import datetime, timedelta
from main import fix_values, populating_vals, flatten, flatten_dupe_vals
from generate_original_with_timeseries import read_endpoint, generate_results
from generate_original_results import results_generator






if __name__ == '__main__':

    # ############################################### Keys and Authentication #######################################
    # if not os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS'):
    #     os.environ['GOOGLE_ACCOUNT_CREDENTIALS'] = '/home/engineering/keyfile.json'
    gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

    # directory = str(os.path.abspath(os.path.dirname(__file__)))
    directory = os.getcwd()

    ############################################### Instantiating Popelines #######################################
    pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)

    # Optimizely parameters
    headers = {
        'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
    }

    query = open(f'{directory}/existing_experiments.sql').read()
    results = pope.bq_query(query)

    # a little clean up before sending hourly calls
    all_exp = []
    for result in results:
        details = {}
        details['id'] = result['id']
        if result['earliest'] is None:
            # details['earliest'] = None
            details['earliest'] = None
            details['status'] = 'experiment_archived'
        else:
            details['earliest'] = result['earliest']
            details['status'] = result['status']
        details['last_modified'] = result['last_modified']
        all_exp.append(details)

    exp_results = []
    for exp in all_exp:
        if exp['status'] == 'archived' or exp['status'] == 'experiment_archived':  
            if exp['earliest'] is not None:
                start_timestamp = datetime.strftime(exp['earliest'], '%Y-%m-%dT%H:%M:%SZ')[:14] + str('00:00Z')
            else:
                start_timestamp = '2018-01-01T00:00:00Z'
            end_timestamp = datetime.strftime(exp['last_modified'], '%Y-%m-%dT%H:%M:%SZ')[:14] + str('00:00Z')
            exp_results.extend(results_generator(start_timestamp, end_timestamp, experiment_id=exp['id']))

        elif exp['status'] == 'not_started':
            print('no results to be backfilled')

        elif exp['status'] == 'running':
            if exp['earliest'] is not None:
                start_timestamp = datetime.strftime(exp['earliest'], '%Y-%m-%dT%H:%M:%SZ')[:14] + str('00:00Z')
            else:
                start_timestamp = '2018-01-01T00:00:00Z'
            end_timestamp = datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%SZ')
            exp_results.extend(results_generator(start_timestamp, end_timestamp, experiment_id=exp['id']))

        elif exp['status'] == 'paused':
            if exp['earliest'] is not None:
                start_timestamp = datetime.strftime(exp['earliest'], '%Y-%m-%dT%H:%M:%SZ')[:14] + str('00:00Z')
            else:
                start_timestamp = '2018-01-01T00:00:00Z'
            end_timestamp = datetime.strftime(exp['last_modified'], '%Y-%m-%dT%H:%M:%SZ')[:14] + str('00:00Z')
            exp_results.extend(results_generator(start_timestamp, end_timestamp, experiment_id=exp['id']))

        else:
            print(f"Experiment {exp['id']} shows a new experiment status. Need to investigate.")

    pope.write_to_json(file_name=f'{directory}/../uploads/testing_results.json', jayson=exp_results, mode='w')
    pope.write_to_bq(table_name='testing_results', file_name=f'{directory}/../uploads/testing_results.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)    
