import json
import requests
from main import flatten

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
    upload_exp_list = []
    for exp in j_exp:
        exp['project_id'] = project_id
        upload_exp_list.append(flatten(exp, {}, ''))
        
    return upload_exp_list


# generate all projects within account
def generate_projects(project_endpoint, project_headers):
    # get all projects
    j_proj = read_endpoint(endpoint=project_endpoint, headers_set=project_headers)

    # store a list of project metadata
    return j_proj


############################################### Project-Experiment #############################################
# project_endpoint = 'https://api.optimizely.com/v2/projects'

# experiment_id = generate_projects(project_endpoint=p_endpoint)

# upload experiments
# pope.write_to_json(file_name='../uploads/experiments.json', jayson=upload_exp_list, mode='w')
# pope.write_to_bq(table_name='experiments', file_name='../uploads/experiments.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
# print(f"Successfully uploaded experiments for project {p_id}")
    
# upload projects 
# pope.write_to_json(file_name='../uploads/projects.json', jayson=project_list, mode='w')
# pope.write_to_bq(table_name='projects', file_name='../uploads/projects.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
# print("Successfully uploaded all projects.")