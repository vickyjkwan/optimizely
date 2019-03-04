import json
import requests
import popelines
import os
from datetime import datetime, timedelta
from main import fix_values, populating_vals, flatten, flatten_dupe_vals
from generate_original_with_timeseries import read_endpoint, generate_projects


# this function checks the last time a job was run on bq,
# returns the last-run timestamp for the table
def check_bq_ts(table_name, ts_col_name):
    last_upload_ts = pope.find_last_entry(table_name, ts_col_name) + timedelta(hours=-8)
    return last_upload_ts


# this function should get timestamp for each entity (project, experiment, or result), from API,
# returns only those with a timestamp that is later than the benchmark, benchmark_ts
def check_api_ts(api_path, ts_col, benchmark_ts, existing_id):
    
    # compare each of the project's last_modified timestamp to the above last upload_ts
    # select only if last_modified > upload_ts
    updating_entity_id = []
    all_existing = generate_projects(api_path, headers)

    for entity in all_existing:
        entity_ts = datetime.strptime(entity[ts_col], '%Y-%m-%dT%H:%M:%S.%fz') + timedelta(hours=-8)
        if  entity_ts > benchmark_ts:
            updating_entity_id.append(entity['id'])
        elif entity['id'] not in existing_id:
            updating_entity_id.append(entity['id'])

    return updating_entity_id


# this function gathers all new entity dictionaries in a list, returns a format that is friendly to popelines
def generate_new_entity(id_list, api_path, table):
    new_json = []
    for entity_id in id_list:
        if table == 'project':
            entity_info = generate_projects(f'{api_path}/{entity_id}', headers)
            entity_info['upload_ts'] = str(datetime.utcnow())

        # need to add experiment table
        else:
            pass

        new_json.append(entity_info)

    return new_json


if __name__ == '__main__':
    
    if not os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS'):
        os.environ['GOOGLE_ACCOUNT_CREDENTIALS'] = '/home/engineering/keyfile.json'
    gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

    directory = str(os.path.abspath(os.path.dirname(__file__)))
    # directory = os.getcwd()

    pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)
    headers = {
        'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
    }

    ######################### updating projects #########################
    # idea is to:
    # GET [project_id, last_modified_ts]
    # if new_project_id is not in {project_id} set: add to it
    # if new_project_id is in {project_id} set: 
        # if last_modified_ts > bq.last_modified_ts: add [project_id, project_props, last_modified_ts(new), upload_ts]
        # else: do nothing

    project_endpoint = 'https://api.optimizely.com/v2/projects'

    # get the timestamp from bq for project table, at which the job was most recently run
    last_upload_ts = check_bq_ts('projects', 'upload_ts')

    # get projects with last_modified timestamps that are later than the previous ts
    # getting ready to upload
    project_query = open(f'{directory}/existing_projects.sql').read()
    existing_projects = []
    for result in pope.bq_query(project_query):
        existing_projects.append(result[0])

    updating_projects = check_api_ts(api_path=project_endpoint, ts_col='last_modified', benchmark_ts=last_upload_ts.replace(tzinfo=None), existing_id=existing_projects)
    updating_projects_json = generate_new_entity(id_list=updating_projects, api_path='https://api.optimizely.com/v2/projects', table='project')

    # send to bq
    pope.write_to_json(file_name=f'{directory}/../uploads/update_projects.json', jayson=updating_projects_json, mode='w')
    pope.write_to_bq(table_name='projects', file_name=f'{directory}/../uploads/update_projects.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
    print(f"Successfully uploaded updated info for projects.")  

    


