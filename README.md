# bitcoin-databricks-etl

### PROJECT
This is my first project exploring databricks free edition. It consists in adaptating a 100% Python ETL to Databricks Intelligence Platform and explore a few of its features.

### STEPS
1. A local python script makes an API call to an get the latest 30 days of bitcoin prices in BRL and USD and upload it to Databricks landing zone
2. Workflow job detects new files and triggers the transformation SQL script and builds delta table

### PIPELINE FLOW
![](workflow.png)