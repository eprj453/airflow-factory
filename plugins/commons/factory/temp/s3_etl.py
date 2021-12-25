from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

class S3ETLProcess:
    def __init__(self,
                 target_path: str,
                 destination_path: str):
        self.MAIN_BUCKET = "da-datalake-prod"
        self.TARGET_BUCKET = "da-datalake-prod"
        self.target_path = target_path
        self.destination_path = destination_path

    def exist_s3_folder_check(self):
        pass

    def execute_s3_job(self):
        pass

'''
def exist_s3_folder_check(prop, table, item):
    exist_s3_folder_check_task = PythonOperator(
        task_id=f"{table}_{item}_exist_s3_object_delete",
        python_callable=S3.delete_partition_s3_objects,
        op_kwargs={
            "bucket": "dw-brandi",
            "prefix_path": "realtime-log/{}/{}/logs/{}/trusted/year={}/month={}/day={}/hour={}/minute={}/",
            "partition_values": [prop, table, item],
            "execution_date": '{{execution_date.strftime("%Y-%m-%d-%H-%M")}}'
        },
        dag=dag,
        queue="s22",
        task_concurrency=10,
        provide_context=False,
    )
    return exist_s3_folder_check_task
'''





