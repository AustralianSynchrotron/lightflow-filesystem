
from lightflow.models import Dag
from lightflow.tasks import PythonTask
from lightflow_filesystem import NewLineTriggerTask


def print_me(name, data, data_store, signal, **kwargs):
    print(data['lines'])

print_task = PythonTask('print_task', python_callable=print_me)

print_dag = Dag('print_dag', autostart=False)
print_dag.define({print_task: None})

new_line_task = NewLineTriggerTask('new_line_task',
                                   'print_dag',
                                   '/tmp/watch_lines.txt',
                                   aggregate=None,
                                   use_existing=False,
                                   flush_existing=False
                                   )

list_dag = Dag('line_dag')
list_dag.define({new_line_task: None})
