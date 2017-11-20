""" Remove a directory and its file content

This workflow creates a temporary directory

  /tmp/lightflow_test/delete_me

and adds three files. The file names are printed using the GlobTask
before the whole directory is removed by the RemoveTask.

"""

from lightflow.models import Dag
from lightflow.tasks import BashTask
from lightflow_filesystem import MakeDirTask, GlobTask, RemoveTask


# the callback function that prints the content of the temporary directory.
def list_files(files, data, store, signal, context):
    for file in files:
        print('{}\n'.format(file))


mkdir_task = MakeDirTask(name='mkdir_task',
                         paths='/tmp/lightflow_test/delete_me')

files_task = BashTask(name='create_files_task',
                      command='touch a; touch b; touch c',
                      cwd='/tmp/lightflow_test/delete_me')

list_task = GlobTask(name='list_task',
                     paths='/tmp/lightflow_test/delete_me',
                     callback=list_files)

remove_task = RemoveTask(name='remove_task',
                         paths='/tmp/lightflow_test/delete_me')

# create a DAG that creates temporary directory and add three files.
# Then list the names of the files and remove the whole directory.
rm_dag = Dag('remove_dag')
rm_dag.define({
    mkdir_task: files_task,
    files_task: list_task,
    list_task: remove_task
})
