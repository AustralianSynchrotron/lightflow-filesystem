import os
import shutil

from lightflow.queue import JobType
from lightflow.logger import get_logger
from lightflow.models import BaseTask, TaskParameters
from .exceptions import (LightflowFilesystemConfigError, LightflowFilesystemPathError,
                         LightflowFilesystemChownError)

logger = get_logger(__name__)


class ChownTask(BaseTask):
    """ Sets the ownership of files and directories. """
    def __init__(self, name, paths, user=None, group=None,
                 recursive=True, only_dirs=False, *,
                 queue=JobType.Task, force_run=False, propagate_skip=True):
        """ Initialize the change ownership task.

        All task parameters except the name, queue, force_run and propagate_skip
        can either be their native type or a callable returning the native type.

        Args:
            name (str): The name of the task.
            paths: A list of paths representing the files or directories for which
                   the ownership should be changed. This parameter can either be
                   a list of strings or a callable that returns a list of strings.
                   The paths have to be absolute paths, otherwise an exception is thrown.
            user: The system user name or uid of the new owner. This parameter can either
                  be a string, an integer or a callable returning a string or integer.
            group: The group user name or gid of the new owner. This parameter can either
                   be a string, an integer or a callable returning a string or integer.
            recursive: Set to True to recursively change subfolders and files
                       if a path is pointing to a directory. This parameter can either be
                       a Boolean value or a callable returning a Boolean value.
            only_dirs: Set to True to only set the ownership for directories and
                       not for files. This parameter can either be a Boolean value or
                       a callable returning a Boolean value.
            queue (str): Name of the queue the task should be scheduled to. Defaults to
                         the general task queue.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, queue=queue,
                         force_run=force_run, propagate_skip=propagate_skip)

        self.params = TaskParameters(
            paths=paths,
            user=user,
            group=group,
            recursive=recursive,
            only_dirs=only_dirs
        )

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the ChownTask task.

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
            LightflowFilesystemChownError: If an error occurred while the ownership is set

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        params = self.params.eval(data, store)

        if params.user is None and params.group is None:
            raise LightflowFilesystemConfigError(
                'At least the user or the group has to be specified')

        for path in params.paths:
            if os.path.isdir(path):
                if not os.path.isabs(path):
                    raise LightflowFilesystemPathError(
                        'The specified path is not an absolute path')

                try:
                    # set the ownership for the root directory
                    shutil.chown(path, params.user, params.group)

                    # get the files and sub-directories
                    if params.recursive:
                        dir_tree = os.walk(path, topdown=False)
                    else:
                        dir_tree = [(path, [],
                                     [f for f in os.listdir(path)
                                      if os.path.isfile(os.path.join(path, f))])]

                    # iterate over the directory tree and set the ownership
                    for root, dirs, files in dir_tree:
                        if not params.only_dirs:
                            for name in files:
                                shutil.chown(os.path.join(root, name),
                                             params.user, params.group)

                        for name in dirs:
                            shutil.chown(os.path.join(root, name),
                                         params.user, params.group)
                except OSError as e:
                    LightflowFilesystemChownError(e)
            else:
                try:
                    shutil.chown(path, params.user, params.group)
                except OSError as e:
                    LightflowFilesystemChownError(e)
