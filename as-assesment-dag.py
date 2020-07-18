import airflow
from airflow.models import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime,timedelta
default_args = {
    'owner': 'mmenjivar',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['marlon.menjivar@live.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': True,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    #'end_date': datetime(2018, 7, 29),
}

dag=DAG('sa-assesment',
	default_args=default_args,
	#schedule_interval='0 0-23 * * *',
	schedule_interval='@once'
        #schedule_interval=None
	)


dim_aisle=SparkSubmitOperator(
	task_id='dim_aisle',
	application = '/home/mmenjivar/SA-Assesment/dimensionsProcess/dim-aisle.py',
	conn_id = 'spark-local',
	py_files='/home/mmenjivar/SA-Assesment/commonFunctions.zip',
	dag = dag,
)

dim_department=SparkSubmitOperator(
	task_id='dim_department',
	application = '/home/mmenjivar/SA-Assesment/dimensionsProcess/dim-department.py',
	conn_id = 'spark-local',
	py_files='/home/mmenjivar/SA-Assesment/commonFunctions.zip',
	dag = dag,
)

dim_user=SparkSubmitOperator(
	task_id='dim_user',
	application = '/home/mmenjivar/SA-Assesment/dimensionsProcess/dim-user.py',
	conn_id = 'spark-local',
	py_files='/home/mmenjivar/SA-Assesment/commonFunctions.zip',
	dag = dag,
)


dim_product=SparkSubmitOperator(
	task_id='dim_product',
	application = '/home/mmenjivar/SA-Assesment/dimensionsProcess/dim-product.py',
	conn_id = 'spark-local',
	py_files='/home/mmenjivar/SA-Assesment/commonFunctions.zip',
	dag = dag,
)


fact_order_items=SparkSubmitOperator(
	task_id='fact_order_items',
	application = '/home/mmenjivar/SA-Assesment/factProcess/fact-order-items.py',
	conn_id = 'spark-local',
	py_files='/home/mmenjivar/SA-Assesment/commonFunctions.zip',
	dag = dag,
)

copy_to_redshift=SparkSubmitOperator(
	task_id='copy_to_redshift',
	application = '/home/mmenjivar/SA-Assesment/redshiftCopyProcess/copyToRedshift.py',
	conn_id = 'spark-local',
	py_files='/home/mmenjivar/SA-Assesment/commonFunctions.zip',
	dag = dag,
)

staging_layer=SparkSubmitOperator(
	task_id='staging_layer',
	application = '/home/mmenjivar/SA-Assesment/commonFunctions/staging-transformations.py',
	conn_id = 'spark-local',
	py_files='/home/mmenjivar/SA-Assesment/commonFunctions.zip',
	dag = dag,
)

dim_order_product_bridge=SparkSubmitOperator(
	task_id='dim_order_product_bridge',
	application = '/home/mmenjivar/SA-Assesment/dimensionsProcess/dim_order_product_bridge.py',
	conn_id = 'spark-local',
	py_files='/home/mmenjivar/SA-Assesment/commonFunctions.zip',
	dag = dag,
)

#Setting dependencies
dim_order_product_bridge.set_upstream([dim_product,dim_aisle,fact_order_items])
dim_aisle.set_upstream([staging_layer])
dim_department.set_upstream([staging_layer])
dim_product.set_upstream([dim_aisle,staging_layer])
dim_user.set_upstream([staging_layer])
fact_order_items.set_upstream([dim_aisle,
			       dim_department,
			       dim_user,
			       dim_product])
copy_to_redshift.set_upstream([dim_order_product_bridge,])
