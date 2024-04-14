from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

from airflow import DAG

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Airflow DAG
with DAG('hdfs_to_clickhouse', default_args=default_args, schedule_interval='0 06 * * *', catchup=False, tags=['project']) as dag:

    spark_job_do_increment = ("spark-submit --class LoadIncrement "
                 "--master local "
                 "--deploy-mode client "
                 "--driver-memory 1G "
                 "--executor-memory 1G "
                 "--num-executors 1 "
                 "/home/projectjars/big-data-project-assembly-0.1.jar /tmp/source /tmp/inc ")

    ssh_run_do_increment = SSHOperator(
        task_id="spark_run_increment",
        ssh_conn_id="spark_conn",
        command=spark_job_do_increment,
        get_pty=True,
        cmd_timeout=None)

    spark_job_do_datamart = ("spark-submit --class LoadDataMart "
                              "--master yarn "
                              "--deploy-mode client "
                              "--driver-memory 1G "
                              "--executor-memory 1G "
                              "--num-executors 1 "
                              "/home/projectjars/big-data-project-assembly-0.1.jar /tmp/inc /tmp/dm")

    ssh_run_do_datamart = SSHOperator(
        task_id="spark_run_datamart",
        ssh_conn_id="spark_conn",
        command=spark_job_do_datamart,
        get_pty=True,
        cmd_timeout=None)

    spark_job_do_export = ("spark-submit --class ExportToExternalStorage "
                              "--master yarn "
                              "--deploy-mode client "
                              "--driver-memory 1G "
                              "--executor-memory 1G "
                              "--num-executors 1 "
                              "--conf spark.jdbc.password={password} "
                              "/home/projectjars/big-data-project-assembly-0.1.jar /tmp/dm/cars_by_country {target_host_port} default.cars_and_buyers country"
                           .format(target_host_port = Variable.get("ck_target_host_port", default_var=None)
                                   ,password = Variable.get("ck_password", default_var=None)
                                   )
                           )

    ssh_run_do_export = SSHOperator(
        task_id="spark_run_export",
        ssh_conn_id="spark_conn",
        command=spark_job_do_export,
        get_pty=True,
        cmd_timeout=None)



ssh_run_do_increment >> ssh_run_do_datamart >> ssh_run_do_export