# virgin-media


## Overview
This pipeline reads data from a public GCS bucket and emits 2 outfiles named `results.txt.gz` and `resultscomp.txt.gz`. The pipeline uses the Apache Beam model to process the input data. 

### Considerations 
The following changes have been made to the project requirement
- I decided to group the data by year e.g 2018 rather than date e.g 2019-01-10. This was due to there only being 6 rows in the input data and none with the same date satisfying all the conditions (`transaction_amount` greater than `20` and transactions made before the year `2010`) so the results would not have demonstrated a group by and sum. 
- the task specified that output be in the format 
date, total_amount
2011-01-01, 12345.00
however the file was to named `output/results.json.gz`. I therefore changed the outfile name to `output/results.txt.gz` to follow the desired output format. 

### Execution

#### Local (Direct Runner)
Create a virtual env
`python -m venv venv`
Activate env 
`source venv/bin/activate`
Install requirements
`pip install -r requirements.txt`
run pipeline
`python main.py --config-file=/conf/dev.yaml`







