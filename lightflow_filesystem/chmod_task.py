import os

from lightflow.models import BaseTask
from lightflow.logger import get_logger
from .exceptions import LightflowFilesystemPathError, LightflowFilesystemChmodError

logger = get_logger(__name__)


class ChmodTask(BaseTask):
    """ Sets the POSIX permissions of files or directories. """
    def __init__(self, name, path, permission,
                 recursive=True, only_dirs=False,
                 force_run=False, propagate_skip=True):
        """ Initialise the change permission task.

        Args:
            name (str): The name of the task.
            path (str): The path to a file or directory for which the permission
                        should be changed. The path has to be an absolute path,
                        otherwise an exception is thrown.
            permission (str): The POSIX permission as a string (e.g. '755').
            recursive (bool): Set to True to recursively change subfolders and files
                              if the path is pointing to a directory.
            only_dirs (bool): Set to True to only set the permission for directories and
                              not for files.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, force_run, propagate_skip)
        self._path = path
        self._permission = permission
        self._recursive = recursive
        self._only_dirs = only_dirs

    def run(self, data, data_store, signal, **kwargs):
        """ The main run method of the ChmodTask task.

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
            LightflowFilesystemChmodError: If an error occurred while the ownership is set

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        path_perm = int(self._permission, 8)

        if os.path.isdir(self._path):
            if not os.path.isabs(self._path):
                raise LightflowFilesystemPathError(
                    'The specified path is not an absolute path')

            try:
                # set the permission for the root directory
                os.chmod(self._path, path_perm)

                # get the files and sub-directories
                if self._recursive:
                    dir_tree = os.walk(self._path, topdown=False)
                else:
                    dir_tree = [(self._path, [],
                                 [f for f in os.listdir(self._path)
                                  if os.path.isfile(os.path.join(self._path, f))])]

                # iterate over the directory tree and set the POSIX permissions
                for root, dirs, files in dir_tree:
                    if not self._only_dirs:
                        for name in files:
                            os.chmod(os.path.join(root, name), path_perm)

                    for name in dirs:
                        os.chmod(os.path.join(root, name), path_perm)
            except OSError as e:
                LightflowFilesystemChmodError(e)
        else:
            try:
                os.chmod(self._path, path_perm)
            except OSError as e:
                LightflowFilesystemChmodError(e)
