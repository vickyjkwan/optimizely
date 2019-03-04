import json
import requests
import popelines
import os
from datetime import datetime, timedelta
from main import fix_values, populating_vals, flatten, flatten_dupe_vals
from generate_original_with_timeseries import read_endpoint, generate_results


# given a start_timestamp and an end_timestamp, this function returns hourly results by calling the results endpoint at hourly interval, between these timestamps
def results_generator(start_timestamp, end_timestamp, experiment_id, headers, params, pope, gbq_key):

    now = datetime.strptime(start_timestamp, '%Y-%m-%dT%H:%M:%SZ')
    exp_results = []
    
    while now < datetime.strptime(end_timestamp,'%Y-%m-%dT%H:%M:%SZ'):
        start_ts = datetime.strftime(now, '%Y-%m-%dT%H:%M:%SZ')
        end_ts = datetime.strftime(now + timedelta(hours=1), '%Y-%m-%dT%H:%M:%SZ')
        params = (
            ('per_page', 100),
        ) 
        print(f"start time is {start_ts}, end time is {end_ts}")

        result_endpoint = f"https://api.optimizely.com/v2/experiments/{experiment_id}/results?start_time={start_ts}&end_time={end_ts}"
        response = read_endpoint(result_endpoint, headers_set=headers, params_set=params)
        print(f"got experiment {experiment_id}")

        # if '' then the experiment has not started yet
        if response.text == '' or 'bad' in response:
            j_ts = {'experiment_id': experiment_id}
            new_j_ts = j_ts
            new_j_ts['upload_ts'] = str(datetime.utcnow())
            new_j_ts['metric_calculating_ts'] = end_ts
            exp_results.append(new_j_ts)

        else:

            new_j_ts = pope.fix_json_values(callback=fix_values, obj=json.loads(response.text), reset_key='results')
            newer_j_ts = pope.fix_json_values(callback=fix_values, obj=new_j_ts, reset_key='variations')
            flattened_j_ts = generate_results(newer_j_ts)
            exp_results.extend(flattened_j_ts)

        now += timedelta(hours=1)
        
    return exp_results


if __name__ == '__main__':

    # ############################################### Keys and Authentication #######################################
    if not os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS'):
        os.environ['GOOGLE_ACCOUNT_CREDENTIALS'] = '/home/engineering/keyfile.json'
    # gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

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

    query = open(f'{directory}/existing_experiments.sql').read()
    results = pope.bq_query(query)

    # a little clean up before sending hourly calls
    all_exp = []
    for result in results:
        details = {}
        details['id'] = result['id']
        if result['earliest'] is None:
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
                end_timestamp = datetime.strftime(exp['last_modified'], '%Y-%m-%dT%H:%M:%SZ')[:14] + str('00:00Z')
                exp_results.extend(results_generator(start_timestamp, end_timestamp, experiment_id=exp['id'], headers=headers, params=params, pope=pope, gbq_key=gbq_key))

        elif exp['status'] == 'not_started':
            print('no results to be backfilled')

        elif exp['status'] == 'running':
            if exp['earliest'] is not None:
                start_timestamp = datetime.strftime(exp['earliest'], '%Y-%m-%dT%H:%M:%SZ')[:14] + str('00:00Z')
                end_timestamp = datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%SZ')
                exp_results.extend(results_generator(start_timestamp, end_timestamp, experiment_id=exp['id'], headers=headers, params=params, pope=pope, gbq_key=gbq_key))

        elif exp['status'] == 'paused':
            if exp['earliest'] is not None:
                start_timestamp = datetime.strftime(exp['earliest'], '%Y-%m-%dT%H:%M:%SZ')[:14] + str('00:00Z')
                end_timestamp = datetime.strftime(exp['last_modified'], '%Y-%m-%dT%H:%M:%SZ')[:14] + str('00:00Z')
                exp_results.extend(results_generator(start_timestamp, end_timestamp, experiment_id=exp['id'], headers=headers, params=params, pope=pope, gbq_key=gbq_key))

        else:
            print(f"Experiment {exp['id']} shows a new experiment status. Need to investigate.")

    pope.write_to_json(file_name=f'{directory}/../uploads/results.json', jayson=exp_results, mode='w')
    pope.write_to_bq(table_name='results', file_name=f'{directory}/../uploads/results.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)    


