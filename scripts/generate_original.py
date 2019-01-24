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

def main():
 
############################################### Project-Experiment #############################################

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
        pope.write_to_json(file_name='../uploads/experiments.json', jayson=upload_exp_list, mode='w')
        pope.write_to_bq(table_name='experiments', file_name='../uploads/experiments.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        print(f"Successfully uploaded experiments for project {p_id}")

        project_list.append(project)

    pope.write_to_json(file_name='../uploads/projects.json', jayson=project_list, mode='w')
    pope.write_to_bq(table_name='projects', file_name='../uploads/projects.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
    print("Successfully uploaded all projects.")

############################################### Experiment-Results #############################################

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
        

        # results_duped = []
        # other_metric_keys = ['aggregator', 'event_id', 'name', 'scope', 'winning_direction']
        # other_experiment_keys = ['confidence_threshold', 'end_time', 'experiment_id', 'start_time', 'stats_config']

        # for metric in j_ts['metrics']:
        #     for key, value in metric.items():
        #         if key == 'results':
        #             d = {}
        #             for result_key, result_value in value.items():
        #                 d[result_key] = flatten(result_value, {}, '')
                        
        #     for k, v in d.items():
        #         new_d = {}
        #         new_d['result_id'] = k
        #         new_d['result'] = v
        #         for key in other_metric_keys:
        #             new_d[key] = metric[key]
        #         for key in other_experiment_keys:
        #             new_d[key] = j_ts[key]
        #         results_duped.append(new_d)
            
        # new_result = []
        # for result in results_duped:
        #     new_result.append(flatten(result, {}, ''))
        
        # upload results 
        pope.write_to_json(file_name='../uploads/result_ts.json', jayson=new_result, mode='w')
        pope.write_to_bq(table_name='result_ts', file_name='../uploads/result_ts.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        print(f"Successfully uploaded result time series for all experiments in experiment {experiment_id}.")


if __name__ == '__main__':

    gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

    pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)

    headers = {
        'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
    }

    main()