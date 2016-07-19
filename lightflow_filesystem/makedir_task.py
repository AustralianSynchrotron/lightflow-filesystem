import os
from lightflow.models import BaseTask, Action
from lightflow.logger import get_logger

logger = get_logger(__name__)


class AbsolutePathError(RuntimeError):
    pass


class MakeDirTask(BaseTask):
    """ Creates a new directory of it does not exist yet. """
    def __init__(self, name, path, force_run=False, propagate_skip=True):
        """ Initialise the MakeDir task.

        Args:
            name (str): The name of the task.
            path (str): The path of the directory that should be created. The path
                        has to be an absolute path, otherwise an exception is thrown.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, force_run, propagate_skip)
        self._path = path

    def run(self, data, data_store, signal, **kwargs):
        """ The main run method of the MakeDir task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.

        Raises:
            AbsolutePathError: If the specified directory is not an absolute path.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        self.makedirs(self._path)
        return Action(data)

    @staticmethod
    def makedirs(path):
        """ Creates a new directory if it doesn't exist yet.

        This is a static method and thus also available for Python callable methods in
        workflows not using the makedir task itself.

        Args:
            path (str): The path of the directory that should be created. The path
                             has to be an absolute path, otherwise an exception is thrown.

        Raises:
            NotAbsolutePath: If the specified directory is not an absolute path.
        """
        if not os.path.isabs(path):
            raise AbsolutePathError()

        if not os.path.exists(path):
            os.makedirs(path)
        else:
            logger.info('Directory {} already exists. Skip creation.'.format(path))
