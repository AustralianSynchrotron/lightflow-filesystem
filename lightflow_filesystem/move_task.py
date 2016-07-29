import os
import shutil

from lightflow.logger import get_logger
from lightflow.models import BaseTask, Action, TaskParameters
from .exceptions import LightflowFilesystemPathError, LightflowFilesystemMoveError

logger = get_logger(__name__)


class MoveTask(BaseTask):
    """ Moves a list of files or folders from a source to a destination. """
    def __init__(self, name, sources, destination, force_run=False, propagate_skip=True):
        """ Initialise the Move task.

        Args:
            name (str): The name of the task.
            sources: A list of file or directory paths that should be moved. This
                    parameter can either be a list of strings or a callable
                    returning a list of strings. The paths have to be absolute
                    paths, otherwise an exception is thrown.
            destination: The destination file or folder the source should be
                         moved to. This parameter can either be a string or a
                         callable returning a string.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, force_run, propagate_skip)
        self.params = TaskParameters(
            sources=sources,
            destination=destination
        )

    def run(self, data, data_store, signal, **kwargs):
        """ The main run method of the MoveTask task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.

        Raises:
            LightflowFilesystemPathError: If the source is a directory
                                          but the target is not.
            LightflowFilesystemMoveError: If the move process failed.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        params = self.params.eval(data, data_store)
        for source in params.sources:
            logger.info('Move {} to {}'.format(source, params.destination))

            if not os.path.isabs(source):
                raise LightflowFilesystemPathError(
                    'The source path is not an absolute path')

            if not os.path.isabs(params.destination):
                raise LightflowFilesystemPathError(
                    'The destination path is not an absolute path')

            if os.path.isdir(source) and not os.path.isdir(params.destination):
                raise LightflowFilesystemPathError(
                    'The destination is not a valid directory')

            try:
                shutil.move(source, params.destination)
            except OSError as e:
                raise LightflowFilesystemMoveError(e)

        return Action(data)
