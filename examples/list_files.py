
from lightflow.models import Dag
from lightflow.tasks import PythonTask
from lightflow_filesystem import GlobTask


def print_me(name, data, data_store, signal, **kwargs):
    print(data['files'])


print_task = PythonTask('print_me', python_callable=print_me)

glob_task = GlobTask('glob_me', paths=['/tmp/lightflow_test/'], pattern='**/*.file', recursive=True)

list_dag = Dag('notify_dag')
list_dag.define({glob_task: [print_task]})
