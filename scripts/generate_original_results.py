import popelines
import json
import requests
import os
from main import fix_values, populating_vals, flatten, flatten_dupe_vals


def generate_experiment_results(ts_endpoint, experiment_id):
    # get result time series from all experiments:
    try:
        response_ts = requests.get(ts_endpoint, headers=headers)
        j_ts = json.loads(response_ts.text)
        response_ts.raise_for_status()
        print('Successfully read experiment results data.')
    except requests.exceptions.HTTPError as err:
        print(err)
    
    # with keys properly reset, we need to populate upper level into each row of each list level
    # Trying to flatten inner level 'timeseries'
    flattened_metrics = []
    for metric in new_j_ts['metrics']:
        for ts in metric['results']:
            flattened_timeseries = []
            for element in ts['timeseries']:
                flattened_timeseries.append(flatten(element, {}, ''))

            # Replace old 'timeseries' with new 'flattened_timeseries'
            updated_results = populating_vals(outer_dict=ts, inner_flattened_list=flattened_timeseries, destination_key='timeseries')
            flattened_results = flatten_dupe_vals(vals=updated_results, key='timeseries')

        # Replace old 'metrics' with new 'flattened_results'
        update_metrics = populating_vals(outer_dict=metric, inner_flattened_list=flattened_results, destination_key='results')
        flattened_metrics.extend(flatten_dupe_vals(vals=update_metrics, key='results'))
        
    return flattened_metrics
    
    # upload results 
    pope.write_to_json(file_name='../uploads/results_ts.json', jayson=flattened_metrics, mode='w')
    # pope.write_to_bq(table_name='result_ts', file_name='../uploads/result_ts.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
    # print(f"Successfully uploaded result time series for all experiments in experiment {experiment_id}.")


if __name__ == '__main__':

    gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

    pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)

    headers = {
        'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
    }



    all_ts = []
    ############################################### Experiment-Results #############################################
    for exp_id in experiment_id:
        print(exp_id)
        ts_endpoint = f'https://api.optimizely.com/v2/experiments/{exp_id}/timeseries'
        # generate_experiment_results(ts_endpoint, experiment_id=exp_id)

        response_ts = requests.get(ts_endpoint, headers=headers)
        if response_ts.text == '':
            # if '' then the experiment has not started yet
            j_ts = {'experiment_id': exp_id}
        else:
            j_ts = json.loads(response_ts.text)
 
        new_j_ts = pope.fix_json_values(callback=fix_values, obj=j_ts, reset_key='results')
        # new_j_ts_list = [new_j_ts]
        
        all_ts.append(new_j_ts)

    pope.write_to_json(file_name='test_results_all.json', jayson=all_ts, mode='w')
    pope.write_to_bq(table_name='test_results_all', file_name='test_results_all.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
