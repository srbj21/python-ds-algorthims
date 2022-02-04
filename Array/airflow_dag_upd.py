from airflow import DAG, utils
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.configuration import conf
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import date, datetime, timedelta
from time import gmtime, strftime
import botocore
import boto3
import re
import json, yaml, random

### Fetch Account Id ###

with open('/usr/local/airflow/ssh/variables.json', "r") as json_file:
    data = json.load(json_file)
    aws_account_id = data['AccountId']
    json_file.close()

##### Set required local & S3 paths #####

cicd_bucket_name = f"s3://bucket-eu-west-1-{aws_account_id}-risk-cicd"
yaml_path = cicd_bucket_name + "/rtd/dags/rtd_basel_config/basel_controller_dag_util_v1.1.yaml"

def get_session():
    """
    Get a AWS Session object
    """
    return botocore.session.get_session()


def get_s3_client():
    """
    Get AWS S3 session client object
    """
    return get_session().create_client('s3')


def path_to_bucket_key(path):
    """
    Split a filename or path to bucket and key format
    """
    if not path.startswith('s3://'):
        raise ValueError('Given path is not a proper s3 path please check again: ' + path)
    path = path[5:].split('/')
    bucket = path[0]
    key = '/'.join(path[1:])
    return bucket, key


def read_s3_file(filepath, encoding='utf8'):
    """
    Read the S3 bucket file and return data as string
    """
    client = get_s3_client()
    bucket, key = path_to_bucket_key(filepath)
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode(encoding)


CONFIG_FILE = read_s3_file(yaml_path)
CONFIG_DICT = yaml.safe_load(CONFIG_FILE)

print("****CONFIG_DICT-->****", CONFIG_DICT)

###############################################################
airflow_dag_name = 'rtd_basel_controller_test_lalit'
root_folder = ''

##############################################################

# Set up defaults for the DAG
default_args = {
    'owner': CONFIG_DICT['owner'],
    'depends_on_past': CONFIG_DICT['depends_on_past'],
    'email': CONFIG_DICT['email'],
    'email_on_failure': CONFIG_DICT['email_on_failure'],
    'email_on_retry': CONFIG_DICT['email_on_retry'],
    'queue': CONFIG_DICT['queue'],  # seperate Queue for each team
    'retry_delay': timedelta(minutes=1),
    'retries': CONFIG_DICT['retries'],  # no of retries on dag failure
    'start_date': datetime(2019, 3, 31),
    'end_date': datetime(2099, 12, 31),
    'concurrency': CONFIG_DICT['concurrency'],  # no of tasks can run in parallel
    # we can set 'execution_timeout': for setting max time the task can run
}

DAG = DAG(airflow_dag_name, default_args=default_args, schedule_interval='@once',catchup=False)
#deploy_mode = CONFIG_DICT['deploy_mode']
airflow_local_dir = CONFIG_DICT['airflow_local_dir']
pem_file = CONFIG_DICT['pem_file']
module = CONFIG_DICT['module']
appname = CONFIG_DICT['appname']
source_code_s3folder = cicd_bucket_name + CONFIG_DICT['code_path']


### Fetch IP Address & PEM file ###
with open(CONFIG_DICT['emr_ip_path'], "r") as emr_ip:
    EMR_MASTER_IP_ADDRESS = emr_ip.read()

emr_logon = f"ssh -o StrictHostKeyChecking=no -t -i {pem_file} hadoop@{EMR_MASTER_IP_ADDRESS}"


### Build the necessary functions to generate spark submit statements & the Bash commands ###

def spark_submit_statement(executors=4, code=None):
    code_absolute_path = "code/{CODE}".format(CODE=code)
    return 'cd {ROOT_FOLDER} && zip -r packages.zip utilities/ config/ && spark-submit  --py-files packages.zip --deploy-mode {DEPLOYMODE} ' \
           ' --conf "spark.dynamicAllocation.maxExecutors={EXECUTORS}" --conf "spark.yarn.dist.archives=packages.zip#packages" ' \
           ' --conf "spark.yarn.appMasterEnv.PYTHONPATH=packages" --conf "spark.executorEnv.PYTHONPATH=packages" {PATH_TO_CODE}' \
           ' --module {MODULE}' \
           ' --jsonfilekey {JSONFILEKEY}' \
           ' --awsaccountid {ACCOUNTID}' \
           ' --appname {APPNAME}' \
           ' --db_out {OUTPUT_DB}' \
           ' --run_id {RUNID}'.format(ROOT_FOLDER=root_folder,
                                      PATH_TO_CODE=code_absolute_path,
                                      MODULE=module,
                                      EXECUTORS=executors,
                                      DEPLOYMODE=deploy_mode,
                                      JSONFILEKEY="{{ dag_run.conf['config_json_file'] if dag_run else '' }}",
                                      APPNAME=appname,
                                      ACCOUNTID=aws_account_id,
                                      CODE=code[:-3],
                                      OUTPUT_DB=airflow_dag_name[0:3],
                                      RUNID="{{ dag_run.conf['RUN_ID'] if dag_run else '' }}"
                                      )


##function to return airflow bash_operator based on inputs provided
def gen_bash_operator(dag=DAG, task_id=None, code=None, trigger_rule='all_success', executors=4):
    spark_submit_string = spark_submit_statement(executors=executors, code=code)

    return BashOperator(task_id=task_id,
                        dag=dag,
                        trigger_rule=trigger_rule,
                        bash_command='{CONNECT_TO_CLUSTER} "{SPARK_SUBMIT_STMT}" ' \
                        .format(CONNECT_TO_CLUSTER=emr_logon,
                                SPARK_SUBMIT_STMT=spark_submit_string)
                        )

def generate_dummy_dag(task_id=None,dag=DAG):
    return DummyOperator(task_id=task_id, dag=DAG)

section_list = CONFIG_DICT['run_type']['RET']

start = DummyOperator(task_id="Start", dag=DAG)
end = DummyOperator(task_id="End", dag=DAG)

# Copy all codes & utilities to EMR local
#copy_calc_code_to_local_cmd = f"{emr_logon} aws s3 cp {source_code_s3folder} . --recursive "
#copy_calc_codes_to_emr = BashOperator(task_id='copy_calc_codes_to_emr', bash_command=copy_calc_code_to_local_cmd,
#                                      dag=DAG)
mode_sequential = 'sequential'
mode_parallel = 'parallel'

#start >> copy_calc_codes_to_emr
#previous = copy_calc_codes_to_emr

for section in section_list:
    task=[]
    mode = section_list[section]
    if mode_sequential in mode:
        operator = generate_dummy_dag(task_id='start_'+section+'_'+mode_sequential+'_run', dag=DAG)
        procedure_list = mode[mode_sequential]
        previous >> operator
        previous = operator
        for p in procedure_list:
            sequence_task = generate_dummy_dag(task_id='start_1', dag=DAG)
            previous >> sequence_task
            previous = sequence_task
            sequence_task = generate_dummy_dag(task_id='wait_1', dag=DAG)
            previous >> sequence_task
            previous = sequence_task
            sequence_task = generate_dummy_dag(task_id='final_status', dag=DAG)
            previous >> sequence_task
            previous = sequence_task
    else:
        operator = generate_dummy_dag(task_id='start_' + section + '_' + mode_parallel + '_run', dag=DAG)
        procedure_list = mode[mode_parallel]
        previous >> operator
        previous = operator
        for p in procedure_list:
            task.append(generate_dummy_dag(task_id='start_paralle_1_'+p, dag=DAG))
            task.append(generate_dummy_dag(task_id='wait_paralle_1_' + p, dag=DAG))
            task.append(generate_dummy_dag(task_id='final_status', dag=DAG))
        previous >> task
        previous = task
previous >> end
