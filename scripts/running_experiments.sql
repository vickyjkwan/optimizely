SELECT
  id,
  MAX(upload_ts) AS last_upload_ts
FROM
  `infusionsoft-looker-poc.optimizely.experiments_single_fields`
WHERE
  status = 'running'
GROUP BY
  1