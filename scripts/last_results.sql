SELECT
experiment_id, MAX(metrics_results_upload_ts)
FROM `infusionsoft-looker-poc.optimizely.results` 
WHERE experiment_id = exp_id
GROUP BY 1