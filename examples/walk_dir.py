""" Acquire some simple statistics about the files in a directory

This workflow traverses the specified directory recursively and counts the number of
files and accumulates their total size. Symbolic links are not counted. 

"""
from lightflow.models import Arguments, Option, Dag
from lightflow_filesystem import WalkTask
from lightflow.tasks import PythonTask


# requires the path to the directory as an argument
arguments = Arguments([
    Option('path', help='The path for which the statistics should be acquired', type=str)
])


# set up the acquisition
def setup(data, store, signal, context):
    data['count'] = 0
    data['size'] = 0


# acquire some basic statistics for each file as long as it is not a symbolic link
def acquire_stats(entry, data, store, signal, context):
    if not entry.is_symlink():
        data['count'] += 1
        data['size'] += entry.stat(follow_symlinks=False).st_size


# print the acquired statistics
def print_stats(data, store, signal, context):
    print('Statistics for folder: {}'.format(store.get('path')))
    print('Number files: {}'.format(data['count']))
    print('Total size (bytes): {}'.format(data['size']))


# the task for setting up the data for the workflow
setup_task = PythonTask(name='setup_task',
                        callback=setup)

# traverse a directory and call the statistics callable for each file
walk_task = WalkTask(name='walk_task',
                     path=lambda data, store: store.get('path'),
                     callback=acquire_stats,
                     recursive=True)

# print the acquired statistics
print_task = PythonTask(name='print_task',
                        callback=print_stats)


# create a DAG that runs the setup, walk and print task consecutively.
main_dag = Dag('main_dag')
main_dag.define({
    setup_task: walk_task,
    walk_task: print_task
})
