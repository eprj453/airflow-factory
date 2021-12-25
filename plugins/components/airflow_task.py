class AirflowTask:
    """
    class AirflowTask

    Task의 구성 요소인 Operator, Task의 구성요소 base_args를 인자로 받아 클래스로 가져갑니다.
    Task object로 미리 만들어놓지 않는 이유는 Task object에 DAG를 나중에 지정할 수 없기 때문입니다.

    이렇게 만들어진 AirflowTask Object는 dag_factory에 전달되며,
    DAGFactory의 클래스 메서드인 _add_task_to_dag 메서드에서 비로소 Task Object로 DAG에 할당됩니다.

    """
    def __init__(self, operator, base_args):
        self.operator = operator
        self.base_args = base_args