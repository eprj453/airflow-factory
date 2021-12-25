from typing import Callable
from uuid import uuid4


class BaseOperatorArgs:
    def to_args(self):
        return self.__dict__


class PythonOperatorArgs(BaseOperatorArgs):
    def __init__(self,
                 python_callable: Callable,
                 task_id=None,
                 op_kwargs=None,
                 op_args=None):
        super(BaseOperatorArgs, self).__init__()
        self.task_id = python_callable.__name__ if not task_id else task_id
        self.python_callable = python_callable
        self.op_kwargs = op_kwargs
        self.op_args = op_args


class BashOperatorArgs(BaseOperatorArgs):
    def __init__(self,
                 bash_command: str,
                 env: dict = None,
                 task_id: str = None,
                 output_encoding: str = 'utf-8'):
        super(BashOperatorArgs, self).__init__()
        self.task_id = f"test_{task_id}" if not task_id else task_id
        self.bash_command = bash_command
        self.env = env
        self.output_encoding = output_encoding


class S3ToRedshiftArgs(BaseOperatorArgs):
    def __init__(self,
                 schema: str = 'PUBLIC',
                 task_id: str = None,
                 ):
        pass
