import os
import time
import inotify.adapters as adapters
import inotify.constants as constants

from lightflow.logger import get_logger
from lightflow.tasks import TriggerTask
from lightflow.models import TaskParameters
from .exceptions import LightflowFilesystemPathError


logger = get_logger(__name__)


class NotifyTriggerTask(TriggerTask):
    """ Triggers the execution of a DAG upon file changes in a directory.

    This trigger task watches a specified directory for file changes. After having
    aggregated a given number of file changes it sends a signal to the parent workflow to
    execute the specified DAG. A list of the files that were changed is given to the
    DAG prior to its execution.
    """
    def __init__(self, name, dag_name, path, recursive=True,
                 out_key=None, aggregate=None, skip_duplicate=False,
                 use_existing_files=False,
                 on_file_create=False, on_file_close=True,
                 on_file_delete=False, on_file_move=False,
                 event_trigger_time=None, stop_polling_rate=2,
                 force_run=False, propagate_skip=True):
        """ Initialise the filesystem notify trigger task.

        All task parameters except the name, force_run and propagate_skip can either be
        their native type or a callable returning the native type.

        Args:
            name (str): The name of the task.
            dag_name: The name of the DAG that should be executed after the
                      specified number of file change events has occurred.
            path: The path to the directory that should be watched for filesystem changes.
                  The path has to be an absolute path, otherwise an exception is thrown.
            recursive: Set to True to watch for file system changes in
                       subdirectories of the specified path. Keeps track of
                       the creation and deletion of subdirectories.
            out_key: The key under which the list of files is being stored in the
                      data that is passed to the DAG. The default is 'files'.
            aggregate: The number of events that are aggregated before the DAG
                       is triggered. Set to None or 1 to trigger on each file
                       event occurrence.
            skip_duplicate: Skip duplicated file names. Duplicated entries can occur if
                            the same file is modified before the list of files is handed
                            to the sub dag. Another case is if the parameter
                            use_existing_files is activated and an existing file
                            is modified before the aggregated files are sent to the sub
                            dag.
            use_existing_files: Use the existing files that are located in path for
                                initialising the file list.
            on_file_create: Set to True to listen for file creation events.
            on_file_close: Set to True to listen for file closing events.
            on_file_delete: Set to True to listen for file deletion events.
            on_file_move:  Set to True to listen for file move events.
            event_trigger_time: The waiting time between events in seconds. Set
                                to None to turn off.
            stop_polling_rate: The number of events after which a signal is sent
                               to the workflow to check whether the task
                               should be stopped.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, dag_name, force_run, propagate_skip)

        # set the tasks's parameters
        self.params = TaskParameters(
            path=path,
            recursive=recursive,
            out_key=out_key if out_key is not None else 'files',
            aggregate=aggregate if aggregate is not None else 1,
            skip_duplicate=skip_duplicate,
            use_existing_files=use_existing_files,
            event_trigger_time=event_trigger_time,
            stop_polling_rate=stop_polling_rate,
            on_file_create=on_file_create,
            on_file_close=on_file_close,
            on_file_delete=on_file_delete,
            on_file_move=on_file_move
        )

    def run(self, data, data_store, signal, **kwargs):
        """ The main run method of the NotifyTriggerTask task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.

        Raises:
            LightflowFilesystemPathError: If the specified path is not absolute.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        params = self.params.eval(data, data_store)

        # build notification mask
        on_file_create = constants.IN_CREATE if params.on_file_create else 0x00000000
        on_file_close = constants.IN_CLOSE_WRITE if params.on_file_close else 0x00000000
        on_file_delete = constants.IN_DELETE if params.on_file_delete else 0x00000000
        on_file_move = constants.IN_MOVE if params.on_file_move else 0x00000000
        mask = (on_file_create | on_file_close | on_file_delete | on_file_move)

        if not os.path.isabs(params.path):
            raise LightflowFilesystemPathError(
                'The specified path is not an absolute path')

        if params.recursive:
            notify = adapters.InotifyTree(params.path.encode('utf-8'))
        else:
            notify = adapters.Inotify()
            notify.add_watch(params.path.encode('utf-8'))

        # if requested, pre-fill the file list with existing files
        files = []
        if params.use_existing_files:
            for (dir_path, dir_names, filenames) in os.walk(params.path):
                files.extend([os.path.join(dir_path, filename) for filename in filenames])
                if not params.recursive:
                    break

        polling_event_number = 0
        try:
            for event in notify.event_gen():
                if params.event_trigger_time is not None:
                    time.sleep(params.event_trigger_time)

                # check every stop_polling_rate events the stop signal
                polling_event_number += 1
                if polling_event_number > params.stop_polling_rate:
                    polling_event_number = 0
                    if signal.is_stopped():
                        break

                # in case of an event check whether it matches the mask and call a dag
                if event is not None:
                    (header, type_names, watch_path, filename) = event

                    if (not header.mask & constants.IN_ISDIR) and\
                            (header.mask & mask):
                        new_file = os.path.join(watch_path.decode('utf-8'),
                                                filename.decode('utf-8'))
                        if not params.skip_duplicate or (params.skip_duplicate and
                                                         new_file not in files):
                            files.append(new_file)

                # as soon as enough files have been aggregated call the sub dag
                if len(files) >= params.aggregate:
                    data[params.out_key] = files
                    signal.run_dag(self._dag_name, data=data)
                    del files[:]

        finally:
            if not params.recursive:
                notify.remove_watch(params.path.encode('utf-8'))
