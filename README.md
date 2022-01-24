# apache_beam_tutorial

This repository it's a tutorial to a batch pipeline using Apache beam
deployed to the cloud in GCP  with Dataflow

## code to deploy in GCP

`
python pipeline_to_calculate_avg_session_by_country.py \
--input gs://apache-beam-tuto/data/data.csv \
--output gs://apache-beam-tuto/data/result \
--runner DataflowRunner \
--project apache-beam-tuto \
--staging_location gs://apache-beam-tuto/data/staging \
--temp_location gs://apache-beam-tuto/data/temp \
--region europe-west1 \
--save_main_session
`
