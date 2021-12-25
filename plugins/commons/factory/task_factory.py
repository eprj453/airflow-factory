from typing import Union, List

from airflow.models import BaseOperator
from airflow import DAG

from components.airflow_task import AirflowTask


class TaskFactory:
    """
    class TaskFactory
    DAG Instance와 AirflowTask Object 혹은 list(AirflowTask Object)로
    1 unit의 Task / Tasks를 만드는 클래스입니다.


    """
    @classmethod
    def generate_task(
            cls,
            dag: DAG,
            airflow_tasks: Union[AirflowTask, List[AirflowTask]]) \
            -> Union[BaseOperator, List[BaseOperator]]:

        if type(airflow_tasks) != list:  # 단일 Task
            task = airflow_tasks.operator(
                dag=dag,
                **airflow_tasks.base_args.to_args()
            )
            print(task.task_id)
            return task

        else:
            tasks = []
            for airflow_task in airflow_tasks:
                task = airflow_task.operator(
                    dag=dag,
                    **airflow_task.base_args.to_args()
                )
                tasks.append(task)
            return tasks