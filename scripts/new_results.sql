SELECT
  id,
  MAX(created)
FROM
  `infusionsoft-looker-poc.optimizely.experiments_single_fields`
WHERE
  id = exp_id
GROUP BY 1