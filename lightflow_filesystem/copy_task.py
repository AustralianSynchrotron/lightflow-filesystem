import os
import shutil

from lightflow.models import BaseTask, Action
from lightflow.logger import get_logger
from .exceptions import LightflowFilesystemPathError, LightflowFilesystemCopyError

logger = get_logger(__name__)


class CopyTask(BaseTask):
    """ Copies a file or folder from a source to a destination. """
    def __init__(self, name, source, destination, force_run=False, propagate_skip=True):
        """ Initialise the Copy task.

        Args:
            name (str): The name of the task.
            source (str): The source file or directory that should be copied.
            destination (str): The destination file or folder the source should
                               be copied to.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, force_run, propagate_skip)
        self._source = source
        self._destination = destination

    def run(self, data, data_store, signal, **kwargs):
        """ The main run method of the CopyTask task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.

        Raises:
            TargetDirectoryError: If the source is a directory but the target is not.
            CopyError: If the copy process failed.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        self.copy(self._source, self._destination)
        return Action(data)

    @staticmethod
    def copy(source, destination):
        """ Copies a source file or directory and its sub-directories to a destination.

        This is a static method and thus also available for Python callable methods in
        workflows not using the copy task itself.

        Args:
            source (str): The source file or directory that should be copied.
            destination (str): The destination file or folder the source should
                               be copied to.

        Raises:
            TargetDirectoryError: If the source is a directory but the target is not.
            CopyError: If the copy process failed.
        """
        if os.path.isdir(source):
            if not os.path.isdir(destination):
                raise LightflowFilesystemPathError(
                    'The destination is not a valid directory')

            try:
                shutil.copytree(source, destination)
            except OSError as e:
                raise LightflowFilesystemCopyError(e)
        else:
            try:
                shutil.copy2(source, destination)
            except OSError as e:
                raise LightflowFilesystemCopyError(e)
