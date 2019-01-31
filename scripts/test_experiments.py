from generate_original_experiments import generate_experiments
import popelines
import os

############################################### Keys and Authentication #######################################
gbq_key = os.environ.get('GOOGLE_ACCOUNT_CREDENTIALS')
pope = popelines.popeline(dataset_id='optimizely', service_key_file_loc=gbq_key, directory='.', verbose=False)

# Optimizely parameters
headers = {
    'Authorization': 'Bearer 2:EWAWmaXb4TgtYVU2VvwoEF-9UbJxBahkiFh1633_Oc9nmju7iJis',
}
project_id = 10849384554
# params include project_id (required) and experiments pulling per each request (default only 25)
params = (
    ('project_id', project_id),
    ('per_page', 100),
) 

experiment_endpoint = 'https://api.optimizely.com/v2/experiments'

experiment_endpoint, experiment_headers, experiment_params = experiment_endpoint, headers, params

project_list = []
experiment_id_list = []
for project in j_proj:
    p_id = project['id']
    generate_experiments(project_id, experiment_endpoint, experiment_headers, experiment_params)
project_list.append(project)


# test 
def test_generate_experiments():
    assert len(exps) == 5
