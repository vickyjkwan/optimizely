import json
import requests
import popelines
import os
from datetime import datetime, timedelta
from main import fix_values, populating_vals, flatten, flatten_dupe_vals
from generate_original import read_endpoint, generate_projects
from batch_update_projects import check_bq_ts, check_api_ts, generate_new_entity


if __name__ == '__main__':
    # if not os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS'):
    #     os.environ['GOOGLE_ACCOUNT_CREDENTIALS'] = '/home/engineering/keyfile.json'
    gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')

    # directory = str(os.path.abspath(os.path.dirname(__file__)))

    pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)
    headers = {
        'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
    }

    ######################### updating experiments #########################
    experiment_endpoint = 

    # get the timestamp from bq for experiment table, at which the job was most recently run
    last_upload_ts = check_bq_ts('origin_experiments_single_fields', 'upload_ts')

    # get experiments with last_modified timestamps that are later than the previous ts
    # getting ready to upload
    updating_experiments = check_api_ts(api_path=experiment_endpoint, table='experiment', ts_col='last_modified', benchmark_ts=last_upload_ts.replace(tzinfo=None))
    updating_experiments_json = generate_new_entity(id_list=updating_experiments, api_path='https://api.optimizely.com/v2/projects', table='experiment')

    # send to bq
    # pope.write_to_json(file_name='../uploads/update_experiments.json', jayson=updating_experiments_json, mode='w')
    # pope.write_to_bq(table_name='update_experiments', file_name='../uploads/update_experiments.json', append=True, ignore_unknown_values=False, bq_schema_autodetect=False)
    # print(f"Successfully uploaded updated info for projects.")  
