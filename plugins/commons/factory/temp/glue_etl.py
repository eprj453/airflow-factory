from abc import ABC, ABCMeta, abstractmethod

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from typing import Dict, Any

from commons.utils.aws import Glue, S3
from commons.utils.const.glue import DynamicGlueJobInfo
from commons.utils import util


# class ETLProcess(metaclass=ABCMeta):
#     @abstractmethod
#     def exist_s3_object_delete(self):
#         raise NotImplementedError("this is abstract class's method!")
#
#     @abstractmethod
#     def execute_glue_job(self):
#         raise NotImplementedError("this is abstract class's method!")
#
#     @abstractmethod
#     def generate_etl_process(self):
#         raise NotImplementedError("this is abstract class!!")


class GlueETLProcess:
    def __init__(
            self,
            dag: DAG,
            source_bucket: str,
            source_path: str,
            target_bucket: str,
            target_path: str,
            table: str,
            job_name: str,
            module_name: str,
            class_name: str
    ):
        self.dag = dag
        self.SOURCE_BUCKET = source_bucket,
        self.SOURCE_PATH = source_path,
        self.TARGET_BUCKET = "da-datalake-prod",
        self.TARGET_PATH = target_path,
        self.table = table
        self.job_info = DynamicGlueJobInfo(
            job_name=job_name,
            module_name=module_name,
            class_name=class_name
        )

    def exist_s3_object_delete(self):  # table : brandi / hiver / mami
        exist_s3_object_delete_task = PythonOperator(
            task_id=f"{self.table}_exist_s3_object_delete",
            python_callable=S3.delete_partition_s3_objects,
            op_kwargs={
                "bucket": self.SOURCE_BUCKET,
                "prefix_path": self.SOURCE_PATH,  # "ml-mart/rds/{}/year={}/month={}/day={}/"
                "partition_values": [self.table],
                "execution_date": '{{execution_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d")}}'
            },
            dag=self.dag,
            provide_context=False,
        )
        return exist_s3_object_delete_task

    def execute_glue_job(self):
        execute_glue_job_task = PythonOperator(
            task_id=f"{self.table}_execute_glue_job",
            python_callable=Glue.execute_glue_job,
            op_kwargs=self.job_info.to_args(),
            dag=self.dag,
            provide_context=False
        )
        return execute_glue_job_task

    def generate_etl_process(self):
        return self.exist_s3_object_delete() >> \
               self.execute_glue_job()


class GlueAndPhysicServerETLProcess(GlueETLProcess, ABC):
    def __init__(
            self,
            dag: DAG,
            target_path: str,
            table: str,
            job_name: str,
            module_name: str,
            class_name: str
    ):
        super(GlueETLProcess, self).__init__()
        self.dag = dag
        self.MAIN_BUCKET = "da-datalake-prod"
        self.TARGET_BUCKET = "da-datalake-prod"
        self.TARGET_PATH = target_path,
        self.table = table
        self.job_info = DynamicGlueJobInfo(
            job_name=job_name,
            module_name=module_name,
            class_name=class_name
        )

    def exist_s3_object_delete(self):  # table : brandi / hiver / mami
        exist_s3_object_delete_task = PythonOperator(
            task_id=f"{self.table}_exist_s3_object_delete",
            python_callable=S3.delete_partition_s3_objects,
            op_kwargs={
                "bucket": self.MAIN_BUCKET,
                "prefix_path": "ml-mart/rds/{}/year={}/month={}/day={}/",
                "partition_values": [self.table],
                "execution_date": '{{execution_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d")}}'
            },
            dag=self.dag,
            provide_context=False,
        )
        return exist_s3_object_delete_task

    def execute_glue_job(self):
        execute_glue_job_task = PythonOperator(
            task_id=f"{self.table}_execute_glue_job",
            python_callable=Glue.execute_glue_job,
            op_kwargs=self.job_info.to_args(),
            dag=self.dag,
            provide_context=False
        )
        return execute_glue_job_task

    def exist_folder_check(self, table):
        exist_folder_check_task = PythonOperator(
            task_id=f"{table}_exist_folder_check",
            python_callable=util.exist_folder_check,
            op_kwargs={
                "execution_date": '{{execution_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d")}}',
                "partition_values": [table],
                "table": table
            },
            dag=self.dag
        )
        return exist_folder_check_task

    # jinja template에 변수를 전달하기 위해 부득이하게 context argument를 사용합니다.
    def push_variable_to_jinja(self, table, **context):
        push_variable_to_jinja_task = PythonOperator(
            task_id=f"{table}_push_variable_to_jinja",
            python_callable=util.push_variable_to_jinja,
            op_kwargs={
                "execution_date": '{{execution_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d")}}',
                "partition_values": [table],
                "table": table
            },
            dag=self.dag,
            provide_context=True,
        )

        return push_variable_to_jinja_task

    def execute_jinja_template(self, table):
        execute_jinja_template_task = BashOperator(
            task_id=f"{table}_execute_jinja_template",
            bash_command="s3_download.sh",
            dag=self.dag,
            params={"task_ids": f"{table}_push_variable_to_jinja"},
        )

        return execute_jinja_template_task

    def compare_size(self, table):
        compare_size_task = PythonOperator(
            task_id=f"{table}_compare_size",
            python_callable=util.size_check,
            op_kwargs={
                "execution_date": '{{execution_date.in_timezone("Asia/Seoul").strftime("%Y-%m-%d")}}',
                "partition_values": [table],
                "table": table
            },
            provide_context=False,
            dag=self.dag,
        )

        return compare_size_task

# gep = GlueETLProcess()
