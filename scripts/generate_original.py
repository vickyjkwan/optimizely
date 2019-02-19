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


# generate all projects within account
def generate_projects(project_endpoint, project_headers):
    # get all projects
    j_proj = read_endpoint(endpoint=project_endpoint, headers_set=project_headers)

    return j_proj


if __name__ == '__main__':

    ############################################### Keys and Authentication #######################################
    if not os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS'):
        os.environ['GOOGLE_ACCOUNT_CREDENTIALS'] = '/home/engineering/keyfile.json'
    gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

    directory = str(os.path.abspath(os.path.dirname(__file__)))

    ############################################### Instantiating Popelines #######################################
    pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)

    # Optimizely parameters
    headers = {
        'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
    }

    # endpoints
    project_endpoint = 'https://api.optimizely.com/v2/projects'
    experiment_endpoint = 'https://api.optimizely.com/v2/experiments'


    ############################################### generate and upload all projects ##############################
    all_projects = generate_projects(project_endpoint, headers)
    for project in all_projects:
        project['upload_ts'] = str(datetime.now())

    # upload projects 
    pope.write_to_json(file_name=f'{directory}/../uploads/projects.json', jayson=all_projects, mode='w')
    pope.write_to_bq(table_name='projects', file_name=f'{directory}/../uploads/projects.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)


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

        exp_list = read_endpoint(endpoint=experiment_endpoint, headers_set=headers, params_set=params)
        exp_id_list = []
        for exp in exp_list:
            exp_id_list.append(exp['id'])
        experiment_id_list.extend(exp_id_list)

        ###### Game plan is to separate nested fields from single layer fields, upload them to separate table, then do joins on BQ level
        expanded_exp_list = []
        all_singles = []

        for exp in exp_list:
            print(f"Processing experiment {exp['id']}, of project {project_id}")

            # single layer fields:
            nested_key_list = []
            for k,v in exp.items():
                if isinstance(v, list) or isinstance(v, dict):
                    nested_key_list.append(k)
            
            single_layer_experiment = {}    
            for k,v in exp.items():
                if k not in nested_key_list:
                    k = k.replace('-', '_')
                    single_layer_experiment[k] = exp[k]

            all_singles.append(single_layer_experiment)

            # nested part into separate tables:
            expanded_exp = {}
            
            expanded_exp['experiment_id'] = exp['id']
            expanded_exp['changes'] = exp['changes']

            expanded_metrics = {}
            expanded_metrics['experiment_id'] = exp['id']
            expanded_metrics['metrics'] = exp['metrics']
            expanded_exp['metrics'] = flatten_dupe_vals(vals=expanded_metrics, key='metrics')

            if 'url_targeting' in exp.keys():
                expanded_exp['experiment_id'] = exp['id']
                expanded_exp['url_targeting'] = exp['url_targeting']
                expanded_exp = flatten(expanded_exp, {}, '')
            
            expanded_exp['experiment_id'] = exp['id']
            expanded_exp['variations'] = exp['variations']
            
            flattened_variations = []
            
            for var in expanded_exp['variations']:
                flattened_actions = []
                for action in var['actions']:
                    flattened_changes = []
                    for element in action['changes']:
                        flattened_changes.append(element)

                    # Replace old 'timeseries' with new 'flattened_timeseries'
                    updated_changes = populating_vals(outer_dict=action, inner_flattened_list=flattened_changes, destination_key='changes')
                    new_flattened_changes = flatten_dupe_vals(vals=updated_changes, key='changes')

                update_actions = populating_vals(outer_dict=var, inner_flattened_list=new_flattened_changes, destination_key='actions')
                flattened_actions.extend(flatten_dupe_vals(vals=update_actions, key='actions'))  

            update_variations = populating_vals(outer_dict=expanded_exp, inner_flattened_list=flattened_actions, destination_key='variations')
            flattened_variations.extend(flatten_dupe_vals(vals=update_variations, key='variations'))
            
            expanded_variations = flattened_variations
            
            expanded_exp = expanded_variations
            expanded_exp_list.extend(expanded_exp)

        pope.write_to_json(file_name=f'{directory}/../uploads/origin_experiments_single_fields.json', jayson=all_singles, mode='w')
        pope.write_to_bq(table_name='origin_experiments_single_fields', file_name=f'{directory}/../uploads/origin_experiments_single_fields.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        print(f"Successfully uploaded single layer part for experiment {exp['id']}")  

        pope.write_to_json(file_name=f'{directory}/../uploads/origin_experiments_nested_fields.json', jayson=expanded_exp_list, mode='w')
        pope.write_to_bq(table_name='origin_experiments_nested_fields', file_name=f'{directory}/../uploads/origin_experiments_nested_fields.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        print(f"Successfully uploaded nested part for experiment {exp['id']}")    


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

            flattened_j_ts = []

            flattened_metrics = []
            for metric in new_j_ts['metrics']:
                if 'results' in metric.keys():
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

                else:
                    flattened_metrics = [flatten(new_j_ts, {}, '')]
                
            update_new_j_ts = populating_vals(outer_dict=new_j_ts, inner_flattened_list=flattened_metrics, destination_key='metrics')
            flattened_j_ts.extend(flatten_dupe_vals(vals=update_new_j_ts, key='metrics'))


        pope.write_to_json(file_name=f'{directory}/../uploads/origin_results.json', jayson=flattened_j_ts, mode='w')
        pope.write_to_bq(table_name='results', file_name=f'{directory}/../uploads/origin_results.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        print(f"Successfully uploaded result time series for experiment {experiment_id}")