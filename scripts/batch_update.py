import json
import requests
import popelines
import os
from main import flatten, fix_values
from generate_original import read_endpoint, generate_experiments, generate_projects

def main():
    
    if not os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS'):
        os.environ['GOOGLE_ACCOUNT_CREDENTIALS'] = '/home/engineering/keyfile.json'
    gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

    directory = str(os.path.abspath(os.path.dirname(__file__)))

    headers = {
        'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
    }

    # endpoints
    project_endpoint = 'https://api.optimizely.com/v2/projects'
    experiment_endpoint = 'https://api.optimizely.com/v2/experiments'

    all_projects = generate_projects(project_endpoint, headers)

    project_id_list = []
    for project in all_projects:
        project_id_list.append(project['id'])

    for project_id in project_id_list:
        # params include project_id (required) and experiments pulling per each request (default only 25)
        params = (
            ('project_id', project_id),
            ('per_page', 100),
        ) 

        new_exp = []
        exp_list, exp_id_list = generate_experiments(project_id, experiment_endpoint, headers, params)
        for exp in exp_list:
            # if experiment was created at a timestamp that is later than last upload, record id
            if exp['created'] >= '2018-10-23T17:48:46.952180Z':
                new_exp.append(exp)
        
        pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)
        pope.write_to_json(file_name='../uploads/new_experiments.json', jayson=new_exp, mode='w')
        pope.write_to_bq(table_name='new_experiments_test', file_name='../uploads/new_experiments.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
        print(f"Successfully uploaded new experiments for project {project_id}")


if __name__ == '__main__':
    main()


