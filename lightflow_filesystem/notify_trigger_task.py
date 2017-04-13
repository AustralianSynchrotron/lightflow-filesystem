import os
import re
import time
import inotify.adapters as adapters
import inotify.constants as constants

from lightflow.logger import get_logger
from lightflow.models import BaseTask, TaskParameters
from .exceptions import LightflowFilesystemPathError


logger = get_logger(__name__)


class NotifyTriggerTask(BaseTask):
    """ Triggers a callback function upon file changes in a directory.

    This trigger task watches a specified directory for file changes. After having
    aggregated a given number of file changes it calls the provided callback function
    with a list of the files that were changed.
    """
    def __init__(self, name, path, callback,
                 recursive=True, aggregate=None, skip_duplicate=False,
                 use_existing=False, flush_existing=True, exclude_mask=None,
                 on_file_create=False, on_file_close=True,
                 on_file_delete=False, on_file_move=False,
                 event_trigger_time=None, stop_polling_rate=2, *,
                 force_run=False, propagate_skip=True):
        """ Initialize the filesystem notify trigger task.

        All task parameters except the name, callback, force_run and propagate_skip can
        either be their native type or a callable returning the native type.

        Args:
            name (str): The name of the task.
            path: The path to the directory that should be watched for filesystem changes.
                  The path has to be an absolute path, otherwise an exception is thrown.
            callback (callable): A callable object that is called with the list of files
                                 that have changed. The function definition is
                                 def callback(files, data, store, signal, context).
            recursive (bool): Set to True to watch for file system changes in
                              subdirectories of the specified path. Keeps track of
                              the creation and deletion of subdirectories.
            aggregate (int, None): The number of events that are aggregated before the
                                   callback function is called. Set to None or 1 to
                                   trigger on each file event occurrence.
            skip_duplicate (bool): Skip duplicated file names. Duplicated entries can
                                   occur if the same file is modified before the list
                                   of files is handed to the callback. Another case
                                   is if the parameter 'use_existing' is activated and
                                   an existing file is modified before the aggregated
                                   files are sent to the callback function.
            use_existing (bool): Use the existing files that are located in path for
                                 initializing the file list.
            flush_existing (bool): If 'use_existing' is True, then flush all existing
                                   files without regard to the aggregation setting.
                                   I.e,. all existing files sent to the callback.
            exclude_mask (str): Specifies a regular expression that can be used to exclude
                                files. For example if a detector creates temporary files
                                that should not be sent to the callback function.
            on_file_create (bool): Set to True to listen for file creation events.
            on_file_close (bool): Set to True to listen for file closing events.
            on_file_delete (bool): Set to True to listen for file deletion events.
            on_file_move (bool):  Set to True to listen for file move events.
            event_trigger_time (float, None): The waiting time between events in seconds.
                                              Set to None to turn off.
            stop_polling_rate (float): The number of events after which a signal is sent
                                       to the workflow to check whether the task
                                       should be stopped.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, force_run=force_run, propagate_skip=propagate_skip)

        # set the tasks's parameters
        self.params = TaskParameters(
            path=path,
            recursive=recursive,
            aggregate=aggregate if aggregate is not None else 1,
            skip_duplicate=skip_duplicate,
            use_existing=use_existing,
            flush_existing=flush_existing,
            exclude_mask=exclude_mask,
            event_trigger_time=event_trigger_time,
            stop_polling_rate=stop_polling_rate,
            on_file_create=on_file_create,
            on_file_close=on_file_close,
            on_file_delete=on_file_delete,
            on_file_move=on_file_move
        )
        self._callback = callback

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the NotifyTriggerTask task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            store (DataStoreDocument): The persistent data store object that allows the
                                       task to store data for access across the current
                                       workflow run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.
            context (TaskContext): The context in which the tasks runs.

        Raises:
            LightflowFilesystemPathError: If the specified path is not absolute.
        """
        params = self.params.eval(data, store)

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

        # setup regex
        if isinstance(params.exclude_mask, str):
            regex = re.compile(params.exclude_mask)
        else:
            regex = None

        # if requested, pre-fill the file list with existing files
        files = []
        if params.use_existing:
            for (dir_path, dir_names, filenames) in os.walk(params.path):
                files.extend([os.path.join(dir_path, filename) for filename in filenames])
                if not params.recursive:
                    break

            if regex is not None:
                files = [file for file in files if regex.search(file) is None]

            if params.flush_existing and len(files) > 0:
                if self._callback is not None:
                    self._callback(files, data.copy(), store, signal, context)
                del files[:]

        polling_event_number = 0
        try:
            for event in notify.event_gen():
                if params.event_trigger_time is not None:
                    time.sleep(params.event_trigger_time)

                # check every stop_polling_rate events the stop signal
                polling_event_number += 1
                if polling_event_number > params.stop_polling_rate:
                    polling_event_number = 0
                    if signal.is_stopped:
                        break

                # in case of an event check whether it matches the mask and call a dag
                if event is not None:
                    (header, type_names, watch_path, filename) = event

                    if (not header.mask & constants.IN_ISDIR) and\
                            (header.mask & mask):
                        new_file = os.path.join(watch_path.decode('utf-8'),
                                                filename.decode('utf-8'))

                        add_file = not params.skip_duplicate or \
                            (params.skip_duplicate and new_file not in files)

                        if add_file and regex is not None:
                            add_file = regex.search(new_file) is None

                        if add_file:
                            files.append(new_file)

                # as soon as enough files have been aggregated call the sub dag
                if len(files) >= params.aggregate:
                    chunks = len(files) // params.aggregate
                    for i in range(0, chunks):
                        if self._callback is not None:
                            self._callback(files[0:params.aggregate], data.copy(),
                                           store, signal, context)
                        del files[0:params.aggregate]

        finally:
            if not params.recursive:
                notify.remove_watch(params.path.encode('utf-8'))
