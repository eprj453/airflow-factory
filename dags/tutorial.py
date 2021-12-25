from datetime import datetime, timedelta

from components.operator_selector import OperatorSelector
from components.operator_args import PythonOperatorArgs, BashOperatorArgs
from components.airflow_task import AirflowTask

from commons.factory.dag_factory import DAGFactory
from commons.util import custom_function

task_1 = AirflowTask(
    operator=OperatorSelector.PYTHON(),
    base_args=PythonOperatorArgs(
        task_id="print_hello",
        python_callable=custom_function.print_hello,
        op_kwargs={
            "name": "jack"
        },
    )
)

task_2 = AirflowTask(
    operator=OperatorSelector.PYTHON(),
    base_args=PythonOperatorArgs(
        task_id="print_hello",
        python_callable=custom_function.print_hello,
        op_kwargs={
            "name": "sam"
        },
    )
)


tasks = [task_1, task_2]

default_args = {
    "owner": "parksw2",
    "start_date": datetime(2021, 12, 23, 6, 20, 0),
    "retries": 0,
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=2),  # 조정 필요
}

dag = DAGFactory.generate_dag(
    dag_id='test-dag',
    schedule_interval='0 12 * * *',
    default_args=default_args,
    tasks=tasks
)