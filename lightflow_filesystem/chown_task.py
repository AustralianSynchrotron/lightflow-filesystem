import os
import shutil

from lightflow.models import BaseTask
from lightflow.logger import get_logger
from .exceptions import (LightflowFilesystemConfigError, LightflowFilesystemPathError,
                         LightflowFilesystemChownError)

logger = get_logger(__name__)


class ChownTask(BaseTask):
    """ Sets the ownership of files or directories. """
    def __init__(self, name, path, user=None, group=None,
                 recursive=True, only_dirs=False,
                 force_run=False, propagate_skip=True):
        """ Initialise the change ownership task.

        Args:
            name (str): The name of the task.
            path (str): The path to a file or directory for which the ownership
                        should be changed. The path has to be an absolute path,
                        otherwise an exception is thrown.
            user: The system user name or uid of the new owner.
            group: The group user name or gid of the new owner.
            recursive (bool): Set to True to recursively change subfolders and files
                              if the path is pointing to a directory.
            only_dirs (bool): Set to True to only set the ownership for directories and
                              not for files.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, force_run, propagate_skip)
        self._path = path
        self._user = user
        self._group = group
        self._recursive = recursive
        self._only_dirs = only_dirs

    def run(self, data, data_store, signal, **kwargs):
        """ The main run method of the ChownTask task.

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
            LightflowFilesystemChownError: If an error occurred while the ownership is set

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        if self._user is None and self._group is None:
            raise LightflowFilesystemConfigError(
                'At least the user or the group has to be specified')

        if os.path.isdir(self._path):
            if not os.path.isabs(self._path):
                raise LightflowFilesystemPathError(
                    'The specified path is not an absolute path')

            try:
                # set the ownership for the root directory
                shutil.chown(self._path, self._user, self._group)

                # get the files and sub-directories
                if self._recursive:
                    dir_tree = os.walk(self._path, topdown=False)
                else:
                    dir_tree = [(self._path, [],
                                 [f for f in os.listdir(self._path)
                                  if os.path.isfile(os.path.join(self._path, f))])]

                # iterate over the directory tree and set the ownership
                for root, dirs, files in dir_tree:
                    if not self._only_dirs:
                        for name in files:
                            shutil.chown(os.path.join(root, name),
                                         self._user, self._group)

                    for name in dirs:
                        shutil.chown(os.path.join(root, name),
                                     self._user, self._group)
            except OSError as e:
                LightflowFilesystemChownError(e)
        else:
            try:
                shutil.chown(self._path, self._user, self._group)
            except OSError as e:
                LightflowFilesystemChownError(e)
