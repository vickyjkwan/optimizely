import json
import requests
import popelines
import os
from datetime import datetime
from main import fix_values, populating_vals, flatten, flatten_dupe_vals


# read endpoint, returns a json file of the HTTP request
def read_endpoint(endpoint, headers_set, params_set=None):
    try:
        response = requests.get(endpoint, headers=headers_set, params=params_set)
        response_text = json.loads(response.text)
        response.raise_for_status()

    except requests.exceptions.HTTPError as err:
        print(err)

    return response_text


# generate all experiments given a project id
def generate_experiments(project_id, experiment_endpoint, experiment_headers, experiment_params):
    # get all experiments from one project
    j_exp = read_endpoint(endpoint=experiment_endpoint, headers_set=experiment_headers, params_set=experiment_params)

    # loop for all experiments in this project
    experiment_id_list = []
    upload_exp_list = []
    for exp in j_exp:
        exp['project_id'] = project_id
        exp['upload_ts'] = str(datetime.now())
        upload_exp_list.append(flatten(exp, {}, ''))
        experiment_id_list.append(exp['id'])
        
    return upload_exp_list, experiment_id_list


# generate all projects within account
def generate_projects(project_endpoint, project_headers):
    # get all projects
    j_proj = read_endpoint(endpoint=project_endpoint, headers_set=project_headers)

    return j_proj


if __name__ == '__main__':

    ############################################### Keys and Authentication #######################################
    # if not os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS'):
    #     os.environ['GOOGLE_ACCOUNT_CREDENTIALS'] = '/home/engineering/keyfile.json'
    gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

    # directory = str(os.path.abspath(os.path.dirname(__file__)))

    ############################################### Instantiating Popelines #######################################
    pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)

    # Optimizely parameters
    headers = {
        'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
    }

    # endpoints
    project_endpoint = 'https://api.optimizely.com/v2/projects'
    experiment_endpoint = 'https://api.optimizely.com/v2/experiments'
    # result_endpoint = f'https://api.optimizely.com/v2/experiments/{experiment_id}/timeseries'


    ############################################### generate and upload all projects ##############################
    all_projects = generate_projects(project_endpoint, headers)
    # upload projects 
    # pope.write_to_json(file_name=f'{directory}/../uploads/projects.json', jayson=all_projects, mode='w')
    # pope.write_to_bq(table_name='projects', file_name=f'{directory}/../uploads/projects.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
    pope.write_to_json(file_name='../uploads/projects.json', jayson=all_projects, mode='w')
    pope.write_to_bq(table_name='projects', file_name='../uploads/projects.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
    

    ############################################### generate and upload all experiments ###########################
    # get a list of project_id from all_projects
    project_id_list = []
    for project in all_projects:
        project_id_list.append(project['id'])

    # to accumulate all experiment_id, for 
    experiment_id_list = []

    # loop over all project_id_list to get experiments within each project
    for project_id in project_id_list:
        # params include project_id (required) and experiments pulling per each request (default only 25)
        params = (
            ('project_id', project_id),
            ('per_page', 100),
        ) 

        exp_list, exp_id_list = generate_experiments(project_id, experiment_endpoint, headers, params)
        experiment_id_list.extend(exp_id_list)

        # upload experiments
        # pope.write_to_json(file_name=f'{directory}/../uploads/origin_experiments.json', jayson=exp_list, mode='w')
        # pope.write_to_bq(table_name='experiments', file_name=f'{directory}/../uploads/origin_experiments.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        pope.write_to_json(file_name='../uploads/origin_experiments.json', jayson=exp_list, mode='w')
        pope.write_to_bq(table_name='experiments', file_name='../uploads/origin_experiments.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        print(f"Successfully uploaded experiments for project {project_id}")

    
    ############################################### generate and upload all result time series ########################
    # loop over all experiment_id in experiment_id_list from above
    for experiment_id in experiment_id_list:
        result_endpoint = f'https://api.optimizely.com/v2/experiments/{experiment_id}/timeseries'
        response_ts = requests.get(result_endpoint, headers=headers)
        print(f"got experiment {experiment_id}")
        # if '' then the experiment has not started yet
        if response_ts.text == '' or 'bad' in response_ts.text:
            j_ts = {'experiment_id': experiment_id}
            new_j_ts = j_ts
            new_j_ts['upload_ts'] = str(datetime.now())
            
        else:
            j_ts = json.loads(response_ts.text)

            new_j_ts = pope.fix_json_values(callback=fix_values, obj=j_ts, reset_key='results')

            # flattened_metrics = []
            # for metric in new_j_ts['metrics']:
            #     if 'results' in metric.keys():
            #         for result in metric['results']:
            #             for timeseries in result['timeseries']:
            #                 new_dict = {}
            #                 new_dict['aggregator'] = metric['aggregator']
                            
            #                 if 'name' in metric.keys():
            #                     new_dict['name'] = metric['name']
            #                 else:
            #                     None

            #                 new_dict['scope'] = metric['scope']
            #                 new_dict['winning_direction'] = metric['winning_direction']

            #                 if 'event_id' in metric.keys():
            #                     new_dict['event_id'] = metric['event_id']
            #                 else:
            #                     new_dict['field'] = metric['field']


            #                 new_dict['result_is_baseline'] = result['is_baseline']
            #                 new_dict['result_level'] = result['level']
            #                 new_dict['result_name'] = result['name']
            #                 new_dict['result_variation_id'] = result['variation_id']
            #                 new_dict['result_id'] = result['results_id']

            #                 if 'rate' in timeseries.keys():
            #                     new_dict['time_series_rate'] = timeseries['rate']
            #                 else:
            #                     pass

            #                 new_dict['time_series_time'] = timeseries['time']
            #                 new_dict['time_series_value'] = timeseries['value']
            #                 new_dict['time_series_samples'] = timeseries['samples']
            #                 new_dict['time_series_variance'] = timeseries['variance']

            #                 flattened_metrics.append(new_dict)
                        
            # new_j_ts['metrics'] = flattened_metrics
            flattened_metrics = []
            for metric in new_j_ts['metrics']:
                for ts in metric['results']:
                    flattened_timeseries = []
                    for element in ts['timeseries']:
                        element['upload_ts'] = str(datetime.now())
                        flattened_timeseries.append(flatten(element, {}, ''))

                    # Replace old 'timeseries' with new 'flattened_timeseries'
                    updated_results = populating_vals(outer_dict=ts, inner_flattened_list=flattened_timeseries, destination_key='timeseries')
                    flattened_results = flatten_dupe_vals(vals=updated_results, key='timeseries')

                # Replace old 'metrics' with new 'flattened_results'
                update_metrics = populating_vals(outer_dict=metric, inner_flattened_list=flattened_results, destination_key='results')
                flattened_metrics.extend(flatten_dupe_vals(vals=update_metrics, key='results'))
            

        # pope.write_to_json(file_name=f'{directory}/../uploads/origin_results.json', jayson=[new_j_ts], mode='w')
        # pope.write_to_bq(table_name='results', file_name=f'{directory}/../uploads/origin_results.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        pope.write_to_json(file_name='../uploads/origin_results.json', jayson=flattened_metrics, mode='w')
        pope.write_to_bq(table_name='results', file_name='../uploads/origin_results.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        print(f"Successfully uploaded result time series for experiment {experiment_id}")