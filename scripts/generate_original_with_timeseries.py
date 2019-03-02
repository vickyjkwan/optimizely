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
        response.raise_for_status()

    except requests.exceptions.HTTPError as err:
        print(err)

    return response


# generate all projects within account
def generate_projects(project_endpoint, project_headers):

    j_proj = read_endpoint(endpoint=project_endpoint, headers_set=project_headers)

    return j_proj


############################################### generate and upload all experiments ###########################
def generate_experiments(exp_list):
    ###### Game plan is to separate nested fields from single layer fields, upload them to separate table, then do joins on BQ level
    all_singles = []
    metrics_table = []
    variations_table = []

    for exp in exp_list:
        print(f"Processing experiment {exp['id']}")

        # single layer fields:
        nested_key_list = []
        for k,v in exp.items():
            if isinstance(v, list):
                nested_key_list.append(k)
        
        single_layer_experiment = {}    
        for k,v in exp.items():
            if k not in nested_key_list:
                k = k.replace('-', '_')
                single_layer_experiment[k] = exp[k]
        single_layer_experiment['upload_ts'] = str(datetime.utcnow())

        all_singles.append(flatten(single_layer_experiment, {}, ''))

        # nested part into separate tables:
        # metrics table:

        flattened_metric = []
        for element in exp['metrics']:
            flattened_metric.append(element)

        updated_metric = populating_vals(outer_dict=exp, inner_flattened_list=flattened_metric, destination_key='metrics')
        new_flattened_metric = flatten_dupe_vals(vals=updated_metric, key='metrics')

        metric_list = []
        for metric in new_flattened_metric:
            metric_dict = {}
            metric_dict['metrics_aggregator'] = metric['metrics_aggregator']
            if 'metrics_event_id' in metric.keys():
                metric_dict['metrics_event_id'] = metric['metrics_event_id']
            metric_dict['metrics_scope'] =  metric['metrics_scope']
            metric_dict['metrics_winning_direction'] = metric['metrics_winning_direction']
            metric_dict['experiment_id'] = exp['id']
            metric_dict['upload_ts'] = str(datetime.utcnow())
            metric_list.append(metric_dict)

        metrics_table.extend(metric_list)
    
        # variations table:
        variations = {}
        variations['experiment_id'] = exp['id']
        variations['variations'] = exp['variations']
        variations['upload_ts'] = str(datetime.utcnow())

        flattened_variations = []

        for var in exp['variations']:
            flattened_actions = []  
            
            if len(var['actions']) > 0:
                for action in var['actions']:

                    flattened_changes = []

                    if action['changes'] != []:

                        for element in action['changes']:
                            flattened_changes.append(element)
                        # Replace old 'changes' with new 'flattened_changes'
                        updated_changes = populating_vals(outer_dict=action, inner_flattened_list=flattened_changes, destination_key='changes')
                        new_flattened_changes = flatten_dupe_vals(vals=updated_changes, key='changes')

                        update_actions = populating_vals(outer_dict=var, inner_flattened_list=new_flattened_changes, destination_key='actions')
                        flat = flatten_dupe_vals(vals=update_actions, key='actions')
                        flattened_actions.extend(flat)

                    else:
                        new_actions = flatten_dupe_vals(vals=var, key='actions')
                        flattened_actions.extend(new_actions)
                    
            else:
                other_flat = {}
                for k,v in var.items():
                    if k != 'actions':
                        other_flat['actions'] = []
                        other_flat[k] = v
                flat = [other_flat]
                flattened_actions.extend(flat)

            update_variations = populating_vals(outer_dict=variations, inner_flattened_list=flattened_actions, destination_key='variations')
            flattened_variations.extend(flatten_dupe_vals(vals=update_variations, key='variations'))

        variations_table.extend(flattened_variations)

    return all_singles, metrics_table, variations_table
  

############################################### generate and upload all result time series ########################
def generate_results(results_jayson):  
    # flattened_j_ts = []
    # flattened_metrics = []

    # for metric in results_jayson['metrics']:
        
    #     if 'results' in metric.keys():
    #         flattened_results = []
    #         for ts in metric['results']:

    #             for element in ts['timeseries']:
    #                 flattened_timeseries = []
    #                 element['upload_ts'] = str(datetime.utcnow())
    #                 flattened_timeseries.append(flatten(element, {}, ''))

    #                 # Replace old 'timeseries' with new 'flattened_timeseries'
    #                 updated_results = populating_vals(outer_dict=ts, inner_flattened_list=flattened_timeseries, destination_key='timeseries')
    #                 flattened_results.extend(flatten_dupe_vals(vals=updated_results, key='timeseries'))

    #         # Replace old 'metrics' with new 'flattened_results'
    #         update_metrics = populating_vals(outer_dict=metric, inner_flattened_list=flattened_results, destination_key='results')
    #         flattened_metrics.extend(flatten_dupe_vals(vals=update_metrics, key='results'))

    #     else:
    #         flattened_metrics = [flatten(results_jayson, {}, '')]
    
    # update_new_j_ts = populating_vals(outer_dict=results_jayson, inner_flattened_list=flattened_metrics, destination_key='metrics')
    # flattened_j_ts.extend(flatten_dupe_vals(vals=update_new_j_ts, key='metrics'))

    flattened_j_ts = []

    for metric in results_jayson['metrics']:

        if 'results' in metric.keys():
            flattened_results = []
            for ts in metric['results']:
                flattened_timeseries = []
                ts['upload_ts'] = str(datetime.utcnow())
                flattened_timeseries.append(flatten(ts, {}, ''))

                # Replace old 'metrics' with new 'flattened_results'
                update_metrics = populating_vals(outer_dict=metric, inner_flattened_list=flattened_timeseries, destination_key='results')
                flattened_results.extend(flatten_dupe_vals(vals=update_metrics, key='results'))

        else:
            flattened_results = [flatten(results_jayson, {}, '')]

        update_new_j_ts = populating_vals(outer_dict=results_jayson, inner_flattened_list=flattened_results, destination_key='metrics')
        flattened_j_ts.extend(flatten_dupe_vals(vals=update_new_j_ts, key='metrics'))

    return flattened_j_ts


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
        project['upload_ts'] = str(datetime.utcnow())

    # upload projects 
    # pope.write_to_json(file_name=f'{directory}/../uploads/projects.json', jayson=all_projects, mode='w')
    # pope.write_to_bq(table_name='projects', file_name=f'{directory}/../uploads/projects.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)


    ############################################### generate and upload all experiments ##############################
    # get a list of project_id from all_projects
    project_id_list = []
    for project in all_projects:
        project_id_list.append(project['id'])

    experiment_id_list = []
    origin_single_table = []
    origin_metrics_table = []
    origin_variations_table = []

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

        # all_singles, metrics_table, variations_table = generate_experiments(exp_list)
        # origin_single_table.extend(all_singles)
        # origin_metrics_table.extend(metrics_table)
        # origin_variations_table.extend(variations_table)

    # pope.write_to_json(file_name=f'{directory}/../uploads/origin_experiments_single_fields.json', jayson=origin_single_table, mode='w')
    # pope.write_to_bq(table_name='experiments_single_fields', file_name=f'{directory}/../uploads/origin_experiments_single_fields.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
    # print(f"Successfully uploaded single layer part for experiment {exp['id']}")  

    # pope.write_to_json(file_name=f'{directory}/../uploads/origin_experiments_metrics_table.json', jayson=origin_metrics_table, mode='w')
    # pope.write_to_bq(table_name='experiments_metrics_table', file_name=f'{directory}/../uploads/origin_experiments_metrics_table.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
    # print(f"Successfully uploaded nested part for experiment {exp['id']}")  

    # pope.write_to_json(file_name=f'{directory}/../uploads/origin_experiments_variations_table.json', jayson=origin_variations_table, mode='w')
    # pope.write_to_bq(table_name='experiments_variations_table', file_name=f'{directory}/../uploads/origin_experiments_variations_table.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
    # print(f"Successfully uploaded nested part for experiment {exp['id']}") 
    

    ############################################### generate and upload all experiments ##############################
    # loop over all experiment_id in experiment_id_list from above
    for experiment_id in experiment_id_list:
        result_endpoint = f'https://api.optimizely.com/v2/experiments/{experiment_id}/timeseries'
        response_ts = requests.get(result_endpoint, headers=headers)
        print(f"got experiment {experiment_id}")
        # if '' then the experiment has not started yet
        if response_ts.text == '' or 'bad' in response_ts.text:
            j_ts = {'experiment_id': experiment_id}
            new_j_ts = j_ts
            new_j_ts['upload_ts'] = str(datetime.utcnow())
            pope.write_to_json(file_name=f'{directory}/../uploads/no_results.json', jayson=[new_j_ts], mode='w')
            pope.write_to_bq(table_name='results', file_name=f'{directory}/../uploads/no_results.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
            
        else:
            j_ts = json.loads(response_ts.text)
            new_j_ts = pope.fix_json_values(callback=fix_values, obj=j_ts, reset_key='results')
            flattened_j_ts = generate_results(new_j_ts)

            pope.write_to_json(file_name=f'{directory}/../uploads/results.json', jayson=flattened_j_ts, mode='w')
            pope.write_to_bq(table_name='results', file_name=f'{directory}/../uploads/results.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
            print(f"Successfully uploaded result time series for experiment {experiment_id}")


    