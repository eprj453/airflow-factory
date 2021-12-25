from airflow import DAG

from datetime import datetime, timedelta
import logging
from typing import Any, Dict, Union, List

from components.airflow_task import AirflowTask
from commons.factory.task_factory import TaskFactory


class DAGFactory:

    @classmethod
    def _create_dag(cls,
                    dag_id: str,
                    default_args: Dict[str, Any],
                    schedule_interval: str,
                    **kwargs,
                    ) -> DAG:
        essential_default_args = {'start_date', 'retry'}

        if essential_default_args < set(default_args.keys()):
            logging.warning(f"""
            default_args 필수 인자가 없습니다. default 값으로 생성됩니다.
            {essential_default_args.difference(set(default_args.keys()))}
            """)

        DEFAULT_ARGS = {
            'owner': 'datadev-parksw2',
            'depends_on_past': False,  # 이전 task가 성공해야만 다음 task가 실행된다.
            'start_date': datetime(2021, 1, 1, 9, 0, 0),
            'email_on_failure': False,
            'email_on_retry': False,
            'retry': 1,
            'retry_delay': timedelta(minutes=5),
        }

        DEFAULT_ARGS.update(default_args)

        DAG_ARGS = {
            'default_args': DEFAULT_ARGS,
            'schedule_interval': schedule_interval,
            "max_active_runs": 10,
            "catchup": True
        }

        DAG_ARGS.update(kwargs)

        dag = DAG(dag_id=dag_id, **DAG_ARGS)
        return dag

    @classmethod
    def _add_tasks_to_dag(cls, dag: DAG, tasks: List[AirflowTask]) -> DAG:
        # task generate in here
        dependency_tasks = []
        n = len(dependency_tasks)
        for task in tasks:
            t = TaskFactory.generate_task(dag=dag, airflow_tasks=task)

            if dependency_tasks:
                upstream_task = dependency_tasks[-1]
                upstream_task.set_downstream(t)
                # upstream_task >> t
            dependency_tasks.append(t)

        return dag

    @classmethod
    def generate_dag(cls,
                     dag_id: str,
                     default_args: Dict[str, Any],
                     schedule_interval: str,
                     tasks: Union[List[AirflowTask]],
                     ) -> DAG:

        """


        """

        dag = cls._create_dag(
            dag_id=dag_id,
            default_args=default_args,
            schedule_interval=schedule_interval,
        )
        dag = cls._add_tasks_to_dag(dag=dag, tasks=tasks)
        return dag
