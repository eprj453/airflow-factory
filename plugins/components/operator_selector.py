from airflow.models import BaseOperator

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


class OperatorSelector:
    @classmethod
    def PYTHON(cls):
        return PythonOperator

    @classmethod
    def BASH(cls):
        return BashOperator

    @classmethod
    def S3_TO_REDSHIFT(cls):
        return S3ToRedshiftOperator
