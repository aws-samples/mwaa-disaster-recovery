from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator

def transform_tasks():
    with TaskGroup("transform", tooltip="Transform Tasks") as group:
    
        transform_a = BashOperator(
            task_id='transform_a',
            bash_command='sleep 1'
        )
    
        transform_b = BashOperator(
            task_id='transform_b',
            bash_command='sleep 1'
        )
    
        transform_c = BashOperator(
            task_id='transform_c',
            bash_command='sleep 1'
        )

    return group
