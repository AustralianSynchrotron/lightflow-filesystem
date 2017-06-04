from glob import glob
from os.path import isabs, basename, join as pjoin

from lightflow.queue import JobType
from lightflow.logger import get_logger
from lightflow.models import BaseTask, TaskParameters, Action
from .exceptions import LightflowFilesystemPathError

logger = get_logger(__name__)


class GlobTask(BaseTask):
    """ Returns list of files from path using glob. """
    def __init__(self, name, paths, callback, pattern='*', recursive=False,
                 return_abs=True, *, queue=JobType.Task,
                 callback_init=None, callback_finally=None,
                 force_run=False, propagate_skip=True):
        """ Initialize the glob task object.

        All task parameters except the name, callback, queue, force_run and propagate_skip
        can either be their native type or a callable returning the native type.

        Args:
            name (str): The name of the task.
            paths (str/list): A path, or list of paths, to look in for files. The paths
                              have to be absolute paths, otherwise an exception is thrown.
            callback (callable): A callable object that is called with the result of the
                                 glob operation. The function definition is
                                 def callback(files, data, store, signal, context).
            pattern (str): The glob style pattern to match when returning files.
            recursive (bool): Recursively look for files. Use ** to match any files
                              and zero or more directories and subdirectories.
                              May slow things down if lots of files.
            return_abs (bool): If True return absolute paths,
                               if False return filename only.
            queue (str): Name of the queue the task should be scheduled to. Defaults to
                         the general task queue.
            callback_init (callable): A callable that is called shortly before the task
                                      is run. The definition is:
                                        def (data, store, signal, context)
                                      where data the task data, store the workflow
                                      data store, signal the task signal and
                                      context the task context.
            callback_finally (callable): A callable that is always called at the end of
                                         a task, regardless whether it completed
                                         successfully, was stopped or was aborted.
                                         The definition is:
                                           def (status, data, store, signal, context)
                                         where status specifies whether the task was
                                           success: TaskStatus.Success
                                           stopped: TaskStatus.Stopped
                                           aborted: TaskStatus.Aborted
                                           raised exception: TaskStatus.Error
                                         data the task data, store the workflow
                                         data store, signal the task signal and
                                         context the task context.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, queue=queue,
                         callback_init=callback_init, callback_finally=callback_finally,
                         force_run=force_run, propagate_skip=propagate_skip)

        if isinstance(paths, str):
            paths = [paths]

        self.params = TaskParameters(paths=paths,
                                     pattern=pattern,
                                     recursive=recursive,
                                     return_abs=return_abs,
                                     )
        self._callback = callback

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the glob task.

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

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        params = self.params.eval(data, store)

        if not all([isabs(path) for path in params.paths]):
            raise LightflowFilesystemPathError(
                'The specified path is not an absolute path')

        files = [file if params.return_abs else basename(file) for path in params.paths
                 for file in glob(pjoin(path, params.pattern),
                                  recursive=params.recursive)]

        if self._callback is not None:
            self._callback(files, data, store, signal, context)

        return Action(data)
