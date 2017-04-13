""" List all files in a directory based on their filename pattern 

This workflow prints the full filepath and filename of all files in the folder
'/tmp/lightflow_test/' if they their file extension is '.file'. 

"""

from lightflow.models import Dag
from lightflow.tasks import PythonTask
from lightflow_filesystem import GlobTask


# the callback function that handles the returned files from the glob task. In this
# example it stores them as a list into the data under the key 'files'. Please note
# how the function returns the data in order to pass the modified data on to the
# next task.
def store_files(files, data, store, signal, context):
    data['files'] = files
    return data


# the callback for the task that prints the filenames that were returned by the glob task.
def print_filenames(data, store, signal, context):
    print('\n'.join(data['files']))


# create a GlobTask to find all files with the '.file' extension and a PythonTask to
# print the result.
glob_task = GlobTask(name='glob_task',
                     paths=['/tmp/lightflow_test/'],
                     callback=store_files,
                     pattern='**/*.file',
                     recursive=True)

print_task = PythonTask(name='print_task',
                        callback=print_filenames)

# create a DAG that runs the glob task first and then the print task.
list_dag = Dag('list_dag')
list_dag.define({
    glob_task: print_task
})
