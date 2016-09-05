""" Example workflow demonstrating the filesystem notification trigger.

The workflow defines two DAGs. The DAG 'notify_task' creates the three directories

    /tmp/lightflow_test/input
    /tmp/lightflow_test/output
    /tmp/lightflow_test/backup

and contains a NotifyTriggerTask that listens to files being closed in

    /tmp/lightflow_test/input

It aggregates over five file closing events before starting the second DAG 'files_dag'
with the list of files that were aggregated. The second DAG copies the aggregated files
to the

    /tmp/lightflow_test/backup

directory before setting restrictive permissions. After the files have been copied they
are moved into the

    /tmp/lightflow_test/output

directory and less restrictive permissions are applied. It should be noted that the
workflow is set up such that the file moving operation and the permission change in
the backup directory are happening at the same time.
"""
import os

from lightflow.models import Dag
from lightflow_filesystem import (MakeDirTask, NotifyTriggerTask,
                                  CopyTask, MoveTask, ChmodTask)


# File/Directory notification DAG
mkdir_task = MakeDirTask(name='mkdir_task',
                         paths=['/tmp/lightflow_test/input',
                                '/tmp/lightflow_test/output',
                                '/tmp/lightflow_test/backup'])

notify_task = NotifyTriggerTask(name='notify_task',
                                path='/tmp/lightflow_test/input',
                                dag_name='files_dag',
                                recursive=True,
                                out_key='files',
                                aggregate=5,
                                use_existing=False,
                                skip_duplicate=False,
                                on_file_create=False,
                                on_file_close=True,
                                on_file_delete=False,
                                on_file_move=False
                                )

notify_dag = Dag('notify_dag')
notify_dag.define({mkdir_task: [notify_task]})


# File handling DAG
copy_backup_task = CopyTask(name='copy_backup_task',
                            sources=lambda data, data_store: data['files'],
                            destination='/tmp/lightflow_test/backup')

chmod_backup_task = ChmodTask(name='chmod_backup_task',
                              paths=lambda data, data_store: [os.path.join('/tmp/lightflow_test/backup', os.path.basename(f)) for f in data['files']],
                              permission='400')

move_output_task = MoveTask(name='move_output_task',
                            sources=lambda data, data_store: data['files'],
                            destination='/tmp/lightflow_test/output')

chmod_output_task = ChmodTask(name='chmod_output_task',
                              paths=lambda data, data_store: [os.path.join('/tmp/lightflow_test/output', os.path.basename(f)) for f in data['files']],
                              permission='700')

files_dag = Dag('files_dag', autostart=False)
files_dag.define({copy_backup_task: [chmod_backup_task, move_output_task],
                  move_output_task: chmod_output_task})
