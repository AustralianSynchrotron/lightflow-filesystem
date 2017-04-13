""" Watch for new lines to be added to a text file

This workflow shows how to use the NewLineTriggerTask to perform work when new lines
are appended to a text file.

In the example below, as soon as a new line is appended to the file
'/tmp/lightflow_test/watch_lines.txt', the callback function 'start_print_dag'
is called which, in turn, stores the newly appended lines into the data, starts the
dag 'print_dag' and passes the data on to the dag. 

"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask
from lightflow_filesystem import NewLineTriggerTask


# the callback function that is called as soon as new lines were appended to the text
# file. It stores the new lines into the data and starts the 'print_dag' dag.
def start_print_dag(lines, data, store, signal, context):
    data['lines'] = lines
    signal.start_dag('print_dag', data=data)


# the callback for printing the new lines from the 'print_dag' dag.
def print_lines(data, store, signal, context):
    print('\n'.join(data['lines']))


# create the task that watches for newly appended lines and the associated dag.
new_line_task = NewLineTriggerTask(name='new_line_task',
                                   path='/tmp/lightflow_test/watch_lines.txt',
                                   callback=start_print_dag,
                                   aggregate=None,
                                   use_existing=False,
                                   flush_existing=False
                                   )

list_dag = Dag('line_dag')
list_dag.define({
    new_line_task: None
})


# create the print dag and set its autostart value to false.
print_task = PythonTask(name='print_task',
                        callback=print_lines)

print_dag = Dag('print_dag', autostart=False)
print_dag.define({
    print_task: None
})
