import os
import inotify.adapters as adapters
import inotify.constants as constants

from lightflow.tasks import TriggerTask
from lightflow.logger import get_logger
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
                 data_key=None, aggregate=None,
                 on_file_create=False, on_file_close=True,
                 on_file_delete=False, on_file_move=False,
                 force_run=False, propagate_skip=True):
        """ Initialise the filesystem notify trigger task.

        Args:
            name (str): The name of the task.
            dag_name (str): The name of the DAG that should be executed after the
                            specified number of file change events has occurred.
            path (str): The path to the directory that should be watched for filesystem
                        changes. The path has to be an absolute path, otherwise an
                        exception is thrown.
            recursive (bool): Set to True to watch for file system changes in
                              subdirectories of the specified path. Keeps track of
                              the creation and deletion of subdirectories.
            data_key (str): The key under which the list of files is being stored in the
                            data that is passed to the DAG. The default is 'files'.
            aggregate (int): The number of events that are aggregated before the DAG
                             is triggered. Set to None or 1 to trigger on each file
                             event occurrence.
            on_file_create (bool): Set to True to listen for file creation events.
            on_file_close (bool): Set to True to listen for file closing events.
            on_file_delete (bool): Set to True to listen for file deletion events.
            on_file_move (bool):  Set to True to listen for file move events.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, dag_name, force_run, propagate_skip)
        self._path = path
        self._recursive = recursive
        self._data_key = data_key if data_key is not None else 'files'
        self._aggregate = aggregate if aggregate is not None and aggregate > 0 else 1

        # build notification mask
        on_file_create = constants.IN_CREATE if on_file_create else 0x00000000
        on_file_close = constants.IN_CLOSE if on_file_close else 0x00000000
        on_file_delete = constants.IN_DELETE if on_file_delete else 0x00000000
        on_file_move = constants.IN_MOVE if on_file_move else 0x00000000
        self._mask = (on_file_create | on_file_close | on_file_delete | on_file_move)

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
        if not os.path.isabs(self._path):
            raise LightflowFilesystemPathError(
                'The specified path is not an absolute path')

        if self._recursive:
            notify = adapters.InotifyTree(self._path.encode('utf-8'))
        else:
            notify = adapters.Inotify()
            notify.add_watch(self._path.encode('utf-8'))

        files = []
        try:
            while 1:
                for event in notify.event_gen():
                    if event is not None:
                        (header, type_names, watch_path, filename) = event

                        if (not header.mask & constants.IN_ISDIR) and\
                                (header.mask & self._mask):
                            files.append(os.path.join(watch_path.decode('utf-8'),
                                                      filename.decode('utf-8')))

                        if len(files) >= self._aggregate:
                            data[self._data_key] = files
                            signal.run_dag(self._dag_name, data=data)
                            del files[:]

        finally:
            if not self._recursive:
                notify.remove_watch(self._path.encode('utf-8'))
