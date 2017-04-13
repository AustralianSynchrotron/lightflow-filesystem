import os

from lightflow.logger import get_logger
from lightflow.models import BaseTask, TaskParameters
from .exceptions import LightflowFilesystemPathError, LightflowFilesystemChmodError

logger = get_logger(__name__)


class ChmodTask(BaseTask):
    """ Sets the POSIX permissions of files and directories. """
    def __init__(self, name, paths, permission,
                 recursive=True, only_dirs=False, *,
                 force_run=False, propagate_skip=True):
        """ Initialise the change permission task.

        Args:
            name (str): The name of the task.
            paths: A list of paths representing the files or directories for which
                   the permissions should be changed. This parameter can either be
                   a list of strings or a callable that returns a list of strings.
                   The paths have to be absolute paths, otherwise an exception is thrown.
            permission: The POSIX permission as a string (e.g. '755'). This parameter can
                        either be a string or a callable returning a string.
            recursive: Set to True to recursively change subfolders and files
                       if a path is pointing to a directory. This parameter can either be
                       a Boolean value or a callable returning a Boolean value.
            only_dirs: Set to True to only set the permission for directories and
                       not for files. This parameter can either be a Boolean value or
                       a callable returning a Boolean value.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, force_run=force_run, propagate_skip=propagate_skip)
        self.params = TaskParameters(
            paths=paths,
            permission=permission,
            recursive=recursive,
            only_dirs=only_dirs
        )

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the ChmodTask task.

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
            LightflowFilesystemChmodError: If an error occurred while the ownership is set

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        params = self.params.eval(data, store)
        path_perm = int(params.permission, 8)

        for path in params.paths:
            if os.path.isdir(path):
                if not os.path.isabs(path):
                    raise LightflowFilesystemPathError(
                        'The specified path is not an absolute path')

                try:
                    # set the permission for the root directory
                    os.chmod(path, path_perm)

                    # get the files and sub-directories
                    if params.recursive:
                        dir_tree = os.walk(path, topdown=False)
                    else:
                        dir_tree = [(path, [],
                                     [f for f in os.listdir(path)
                                      if os.path.isfile(os.path.join(path, f))])]

                    # iterate over the directory tree and set the POSIX permissions
                    for root, dirs, files in dir_tree:
                        if not params.only_dirs:
                            for name in files:
                                os.chmod(os.path.join(root, name), path_perm)

                        for name in dirs:
                            os.chmod(os.path.join(root, name), path_perm)
                except OSError as e:
                    LightflowFilesystemChmodError(e)
            else:
                try:
                    os.chmod(path, path_perm)
                except OSError as e:
                    LightflowFilesystemChmodError(e)
