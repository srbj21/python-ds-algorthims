from airflow import DAG, utils
from airflow.configuration import conf
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import date, datetime, time, timedelta
from airflow import AirflowException
from airflow.operators.sensors import S3KeySensor, ExternalTaskSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.models import Variable, XCom
from airflow.models.dagrun import DagRun
from airflow.utils.trigger_rule import TriggerRule
import time as ti
import boto3
import json
import botocore
import json, yaml, random

#Airflow packages import 
from airflow import DAG, utils
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
\


### Fetch Account Id ###

with open('/usr/local/airflow/ssh/variables.json', "r") as json_file:
    data = json.load(json_file)
    aws_account_id = data['AccountId']
    json_file.close()

##### Set required local & S3 paths #####

cicd_bucket_name = f"s3://bucket-eu-west-1-{aws_account_id}-risk-cicd"
yaml_path = cicd_bucket_name + "/rtd/dags/config/basel_controller_wholesale_dag_util.yaml"


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
s3_bucket = path_to_bucket_key(yaml_path)[0]

print("****CONFIG_DICT-->****", CONFIG_DICT)

###############################################################
airflow_dag_name = 'rtd_basel_controller_wholesale'
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

schedule = CONFIG_DICT['schedule']
emr_file_path=CONFIG_DICT["EMR_FILE_PATH"]
etl_param_table = CONFIG_DICT['etl_param_table']
RUN_ID = str('_preprod')
DAG = DAG(airflow_dag_name, default_args=default_args, schedule_interval=schedule, catchup=False)
# deploy_mode = CONFIG_DICT['deploy_mode']
airflow_local_dir = CONFIG_DICT['airflow_local_dir']
pem_file = CONFIG_DICT['pem_file']
module = CONFIG_DICT['module']
appname = CONFIG_DICT['appname']
status_output_path = CONFIG_DICT['status_output_path'] + 'rtd_basel_contoller_wholesale_status' + RUN_ID
run_type_dict = CONFIG_DICT['run_type']
section_list_retail = CONFIG_DICT['run_type']['WSL']
### Fetch IP Address & PEM file ###
with open(CONFIG_DICT['emr_ip_path'], "r") as emr_ip:
    EMR_MASTER_IP_ADDRESS = emr_ip.read()

ssh_cmd = f"ssh -o StrictHostKeyChecking=no -t -i {pem_file} hadoop@{EMR_MASTER_IP_ADDRESS}"


#########################################################################
# DEFINE FUNCTIONS
#########################################################################

def fetch_dag_details(Dagid, exec_def):
    dag_runs = DagRun.find(dag_id=str(Dagid))
    list_state = {}
    for dag_run in dag_runs:
        state = dag_run.state
        exec_date = dag_run.execution_date
        dag_id = dag_run.dag_id
        if exec_date.strftime('%m-%d-%Y %H:%M:%S.%f') == exec_def:
            list_state = {"run_id": RUN_ID,
                          "dag_id": dag_id,
                          "status": state,
                          "start_date": exec_date.strftime('%m-%d-%Y %H:%M:%S.%f')
                          }
            print("The Trigger Dag state : " + str(state) + "  " + str(exec_date))
    return list_state


def loop_dag_status(Dagid, exec_def, **context):
    list_state = fetch_dag_details(Dagid, exec_def)
    while list_state["status"] == "running":
        ti.sleep(120)
        list_state = fetch_dag_details(Dagid, exec_def)
    task_instance = context['task_instance']
    task_instance.xcom_push(key="list_state", value=list_state)
    if list_state['status'] == "failed":
        raise AirflowException("Failing task because one or more upstream tasks failed.")


def write_status_to_s3(output_path, content):
    # Method 1: Object.put()
    s3 = boto3.resource('s3')
    object = s3.Object(s3_bucket, output_path)
    # object.put(Body=(bytes(json.dumps(values).encode('UTF-8'))))
    print("content is : " + str(content))
    print('s3://' + s3_bucket + '/' + output_path)
    object.put(Body=(str(json.dumps(content))))
    print('File saved to s3 location')


def generate_dummy_dag(task_id=None, dag=DAG):
    return DummyOperator(task_id=task_id, dag=DAG)


def trigger(context, dag_run_obj):
    return dag_run_obj


def executin_date(**context):
    exec_date_def = datetime.now().strftime('%m-%d-%Y %H:%M:%S.%f')
    task_instance = context['ti']
    task_instance.xcom_push(key="execution_date_" + RUN_ID, value=exec_date_def)
    file_path = status_output_path + '/rtd_controller_start_status.json'
    value = {airflow_dag_name + RUN_ID: "started",
             "start_date": exec_date_def
             }
    write_status_to_s3(file_path, value)
    return


def final_status(run_type_dict, **kwargs):
    task_dict = {}
    temp_status= 'success'
    end_date = datetime.now().strftime('%m-%d-%Y %H:%M:%S.%f')
    task_dict[airflow_dag_name + RUN_ID] = 'success'
    task_dict['end_date'] = end_date
    for task_instance in kwargs['dag_run'].get_task_instances():
        print("****************************")
        print(task_instance.task_id)
        for run_type in run_type_dict:
            section_list = run_type_dict[run_type]
            for section in section_list:
                mode = section_list[section]
                if mode_sequential in mode:
                    procedure_list = mode[mode_sequential]
                else:
                    procedure_list = mode[mode_parallel]

                for dag_task in procedure_list:
                    if dag_task in task_instance.task_id and task_dict.get(
                            dag_task) != None and task_instance.current_state() != 'success':  # check only those dag status which is in config file
                        print(dag_task)
                        print(task_instance.current_state())
                        task_dict[dag_task] = task_instance.current_state()  # only triggered dag status is captured
                    elif dag_task in task_instance.task_id and task_dict.get(dag_task) == None:
                        print(task_instance.current_state())
                        print(dag_task)
                        task_dict[dag_task] = task_instance.current_state()
            print("printing last block")
            print(task_instance.current_state())
            print(task_instance.task_id)
            print(kwargs['task_instance'].task_id)
            print(task_instance)
            print(kwargs['task_instance'])
            if task_instance.current_state() != 'success' and task_instance.task_id != kwargs['task_instance'].task_id:
                if temp_status =='success':
                    task_dict[airflow_dag_name + RUN_ID] = 'failed'
                    temp_status ='failed'

    file_path = status_output_path + '/rtd_controller_wholesale_end_status.json'
    write_status_to_s3(file_path, task_dict)
    if task_dict[airflow_dag_name + RUN_ID] == 'failed':
        raise Exception("One of of the upstream task has failed. Failing this DAG run")


def check_schedule():
    try:
        wd_schedule = CONFIG_DICT['schedule_wd']
        schedule_date = datetime.strptime(wd_schedule,'%Y-%m-%d').date()
        today = date.today()
        if schedule_date == today:
            pass
        else:
            raise Exception(f"Today is not the batch run scheduled date. Batch run is scheduled on: {wd_schedule}. current date: {today.strftime('%Y-%m-%d')}")
    except Exception as e:
        raise e


def logger_start_wait():
    return ti.sleep(400)


start = DummyOperator(task_id="Start", dag=DAG)


set_execution = PythonOperator(task_id="set_execution",
                               provide_context=True,
                               python_callable=executin_date,
                               dag=DAG)

check_schedule = PythonOperator(task_id="check_schedule",
                               provide_context=False,
                               python_callable=check_schedule,
                               dag=DAG)

logger_service = TriggerDagRunOperator(task_id="logger_service",
                      trigger_dag_id="rtd_logger_service",
                      dag=DAG)

logger_service_wait = PythonOperator(task_id="logger_start_wait",
                               provide_context=False,
                               python_callable=logger_start_wait,
                               dag=DAG)


final_status = PythonOperator(
    task_id='final_status',
    provide_context=True,
    python_callable=final_status,
    op_kwargs={'run_type_dict': run_type_dict},
    trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
    dag=DAG
)


set_run_rk = BashOperator(task_id='set_run_rk',
                          bash_command=(f"""{ssh_cmd} spark-submit -v --master=yarn --driver-memory=19g \
                                            --conf spark.driver.cores=5 \
                                            --conf spark.driver.memoryOverhead=2g \
                                            --conf spark.dynamicAllocation.maxExecutors=16 \
                                            --conf spark.default.parallelism=200  --conf spark.sql.shuffle.partitions=200 \
                                            --conf spark.dynamicAllocation.executorIdleTimeout=300 \
                                            --conf spark.sql.broadcastTimeout=3600 \
                                            {emr_file_path}dim_run_rk.py --process_name wholesale """
                                        .format(ssh_cmd=ssh_cmd,
                                                emr_file_path=emr_file_path,etl_param_table=etl_param_table )
                                        ),
                          dag = DAG)


mode_sequential = 'sequential'
mode_parallel = 'parallel'

start >> check_schedule >> logger_service >> logger_service_wait >> set_execution>>set_run_rk

previous = set_run_rk

for run_type in run_type_dict:
    previous_task= previous
    section_list =run_type_dict[run_type]
    print("here is Lalit")
    print(section_list)
    for section in section_list:
        task = []
        task1 = []
        mode = section_list[section]
        if mode_sequential in mode:
            operator = generate_dummy_dag(task_id='start_' + section + '_' + mode_sequential + '_run', dag=DAG)
            #if  run_type=='RET' and section=='section_ret_3':
            #   ret_post_calc_dag=operator
            procedure_list = mode[mode_sequential]
            previous_task >> operator
            previous_task = operator
            for p in procedure_list:
                sequence_task = TriggerDagRunOperator(task_id=p,
                                                      trigger_dag_id=procedure_list[p],
                                                      python_callable=trigger,
                                                      retries=3,
                                                      retry_delay=timedelta(minutes=5),
                                                      execution_date="{{{{ ti.xcom_pull(task_ids='set_execution',key='execution_date_{}') }}}}".format(
                                                          RUN_ID),
                                                      dag=DAG)
                
                previous_task >> sequence_task
                previous_task = sequence_task
                sequence_task = PythonOperator(task_id=p + "_wait",
                                               provide_context=True,
                                               python_callable=loop_dag_status,
                                               op_kwargs={'Dagid': procedure_list[p],
                                                          'exec_def': "{{{{ ti.xcom_pull(task_ids='set_execution',key='execution_date_{}') }}}}".format(
                                                              RUN_ID)},
                                               dag=DAG)
                #if run_type=='RET_CRM' and section=='section_crm_3':
                #   ret_post_crm_wait_dag=sequence_task
                previous_task >> sequence_task
                previous_task = sequence_task
                final_status.set_upstream(sequence_task)


        else:
            operator = generate_dummy_dag(task_id='start_' + section + '_' + mode_parallel + '_run', dag=DAG)
            procedure_list = mode[mode_parallel]
            previous_task >> operator
            previous_task = operator
            for p in procedure_list:
                t1 = TriggerDagRunOperator(task_id=p,
                                           trigger_dag_id=procedure_list[p],
                                           python_callable=trigger,
                                           retries=3,
                                           retry_delay=timedelta(minutes=5),
                                           execution_date="{{{{ ti.xcom_pull(task_ids='set_execution',key='execution_date_{}') }}}}".format(
                                               RUN_ID),
                                           dag=DAG)
                
                t2 = PythonOperator(task_id=p + "_wait",
                                    provide_context=True,
                                    python_callable=loop_dag_status,
                                    op_kwargs={'Dagid': procedure_list[p],
                                               'exec_def': "{{{{ ti.xcom_pull(task_ids='set_execution',key='execution_date_{}') }}}}".format(
                                                   RUN_ID)},
                                    dag=DAG)
                t1 >> t2
                final_status.set_upstream(t2)
                task.append(t1)
                task1.append(t2)
            previous_task >> task
            previous_task = task1
    previous_task >> final_status
