
# Project: Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

# **DAG**
![enter image description here](https://lh3.googleusercontent.com/pw/AIL4fc9Te9uMkmWq3sjpC7TTw_8li8Y1HHC-Eddajcf9WFIUp8E0OcvCTTj8wt461VI8YHOa3Q_hO05CK757vMbWCRJos2XSUEtU4TMDIudWhxkTmumc54Wk3AhBhr5jcIxZEmUQAgOlR-Jv3BbnQU91JrPE=w1366-h373-s-no?authuser=0)
# **Table Schema**
![enter image description here](https://raw.githubusercontent.com/hiep1810/Udacity-nd027-Data-Warehouse/main/images/schema.png)
# **How To Run**

Set environment variables  `aws_credentials`  and  `redshift` with AWS URI and Redshift URI.

**Set AIRFLOW_HOME**
```python
export AIRFLOW_HOME='/home/workspace/airflow'
```

**Add a directory to PYTHONPATH**
```python
export PYTHONPATH="${PYTHONPATH}:/home/workspace/airflow/dags:/home/workspace/airflow/logs:/home/workspace/airflow/plugins"
```
**Start the Airflow Web Server**
```bash
airflow scheduler
```
```bash
airflow webserver -p 3000
```
**Set connections and variables**
```bash
/home/workspace/set_connections_and_variables.sh

```
