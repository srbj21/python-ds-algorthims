import boto3
import json
import yaml
import botocore.session
try:
    # for Python 2.x
    from StringIO import StringIO
except ImportError:
    # for Python 3.x
    from io import StringIO
from time import sleep
from pytz import utc, timezone
from datetime import date, datetime, time, timedelta


#Airflow packages import 
from airflow import DAG, utils
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

#AWS Session object
def get_session():
	return botocore.session.get_session()

#AWS S3 Session Client object
def get_s3_client():
	return get_session().create_client('s3')

#Create bucket and key by splitting filename
def path_to_bucket_key(path):   
	if not path.startswith('s3://'):
		raise ValueError('S3 path provided is incorrect: ' + path)
	path = path[5:].split('/')
	bucket = path[0]
	key = '/'.join(path[1:])
	return bucket, key

#Read the S3 bucket file and reeturn data as string
def read_s3_file(filepath, encoding='utf8'): 
	client = get_s3_client()
	bucket, key = path_to_bucket_key(filepath)
	obj = client.get_object(Bucket=bucket, Key=key)
	return obj['Body'].read().decode(encoding)

#Extract the AWS account id
with open('/usr/local/airflow/ssh/variables.json') as json_file:
	data = json.load(json_file) 
aws_account_id = data['AccountId']
json_file.close()

#Config file attributes
yaml_path = "s3://bucket-eu-west-1-"+aws_account_id+"-risk-cicd/rtd/dags/config/rtd_rwa_calculator_master_config.yaml"
CONFIG_FILE = read_s3_file(yaml_path)
CONFIG_FILE = CONFIG_FILE.format(account=aws_account_id)
CONFIG_DICT = yaml.safe_load(CONFIG_FILE)
KEY_FILE = CONFIG_DICT["KEY_FILE"]
IP = open(CONFIG_DICT["IP_FILE"],"r").read()
emr_file_path=CONFIG_DICT["EMR_FILE_PATH"]
queue=CONFIG_DICT["QUEUE"]
hql_path=CONFIG_DICT["HQL_PATH"]

#Defining the default arguments of DAG
default_args = {
	'owner': 'bvsbb',
	'depends_on_past': False,
	'email': ['shreyas.bv@natwest.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'queue': queue, 
	'retry_delay': timedelta(minutes = 1),
	'retries' : 1, 
	'start_date': datetime(2021, 3, 25),
	'end_date': datetime(2099, 12, 31), 
	'concurrency': 1 ,
	}

#Dag Creation
dag = DAG('rtd_test_logger_load_table', default_args = default_args, schedule_interval = None)

#Define emr logon command
ssh_cmd = ("ssh -o StrictHostKeyChecking=no -t -i {KEY} hadoop@{IP} "
			.format(KEY = KEY_FILE, 
					IP = IP)
			)

#Define DAGs
#Define dummy tasks for start and end of flow
start_task = DummyOperator(task_id='Start_Flow', dag=dag)
								 
								
#Define task to run PySpark script on EMR
logger_load_task = BashOperator(task_id='logger_load_task', 
								bash_command=('{ssh_cmd} sudo spark-submit {emr_file_path}code/logger_test_load.py {emr_file_path}'
									.format(ssh_cmd = ssh_cmd,emr_file_path=emr_file_path)),dag=dag)



start_task >> logger_load_task  >> stop_task
