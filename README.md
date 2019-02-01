# Optimizely

Optimizely is a software for designing and storing simple statistical experiments on webpage traffics and impressions. This ETL extracts following tables:

- Projects: 
Each Optimizely account can start multiple projects.

- Experiments:
Within each project, designers and analysts can start, pause, archive and customize various marketing experiments, majority of which are A/B testing. This ETL extracts all historic and currently active experiments. When there is a new experiment, or a change of status to any experiment, it will be reflected within x hours on BigQuery.

- Result Time Series:
For each Active, Paused, Archived (after activated) experiments, a series of results with timestamps and other metrics and configuration parameters will be real-time updated on BQ. When there's new result to this time series data, it will be reflected within x hours on BQ.
