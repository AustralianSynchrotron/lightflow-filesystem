""" Example workflow demonstrating the filesystem notification trigger.

This workflow waits for file system changes in the '/tmp/lightflow_test' directory. As
soon as five files have been closed after writing, it prints the path and filename
to the command line.

The workflow defines two DAGs. The DAG 'notify_task' contains a NotifyTriggerTask
that listens to files being closed in the '/tmp/lightflow_test' folder. It aggregates
over five file closing events before starting the second DAG 'print_files' with the
list of files that were aggregated.
"""
from lightflow.models import Dag
from lightflow.tasks import PythonTask
from lightflow_filesystem import NotifyTriggerTask


# File/Directory notification DAG
notify_task = NotifyTriggerTask(name='notify_task',
                                dag_name='files_dag',
                                path='/tmp/lightflow_test',
                                recursive=True,
                                data_key='files',
                                aggregate=5,
                                on_file_create=False,
                                on_file_close=True,
                                on_file_delete=False,
                                on_file_move=False
                                )

notify_dag = Dag('notify_dag')
notify_dag.define({notify_task: None})


# File handling DAG
print_files = PythonTask(name='print_files',
                         python_callable=
                         lambda name, data, data_store, signal: print(data['files']))

files_dag = Dag('files_dag', autostart=False)
files_dag.define({print_files: None})
