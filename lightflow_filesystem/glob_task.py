from os.path import isabs, basename, join as pjoin

from glob import glob
from lightflow.logger import get_logger
from lightflow.models import BaseTask, TaskParameters, Action
from .exceptions import LightflowFilesystemPathError

logger = get_logger(__name__)


class GlobTask(BaseTask):
    """ Returns list of files from path using glob. """
    def __init__(self, name, paths, pattern='*', out_key='files', recursive=False,
                 return_abs=True, *, force_run=False, propagate_skip=True):
        """

        Args:
            name (str): The name of the task.
            paths (str/list): A path, or list of paths, to look in for files. The paths have
                              to be absolute paths, otherwise an exception is thrown.
            pattern (str): The glob style pattern to match when returning files.
            out_key (str): The key under which the list of files is being stored in the
                            data that is passed to the DAG. The default is 'files'.
            recursive (bool): Recursively look for files. Use ** to match any files and zero or
                              more directories and subdirectories. May slow things down if lots of files.
            return_abs (bool): If True return absolute paths, if False return filename only.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        if isinstance(paths, str):
            paths = [paths]
        super().__init__(name, force_run=force_run, propagate_skip=propagate_skip)
        self.params = TaskParameters(paths=paths,
                                     pattern=pattern,
                                     out_key=out_key,
                                     recursive=recursive,
                                     return_abs=return_abs)

    def run(self, data, store, signal, **kwargs):
        """

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            store (DataStoreDocument): The persistent data store object that allows the
                                       task to store data for access across the current
                                       workflow run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.

        Raises:
            LightflowFilesystemPathError: If the specified path is not absolute.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        params = self.params.eval(data, store)
        if not all([isabs(path) for path in params.paths]):
            raise LightflowFilesystemPathError('The specified path is not an absolute path')

        data[params.out_key] = [file if params.return_abs else basename(file) for path in params.paths
                                for file in glob(pjoin(path, params.pattern), recursive=params.recursive)]

        return Action(data)
