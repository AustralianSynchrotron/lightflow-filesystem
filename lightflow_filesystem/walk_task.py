from os import scandir
from os.path import isabs

from lightflow.queue import JobType
from lightflow.logger import get_logger
from lightflow.models import BaseTask, TaskParameters, Action
from .exceptions import LightflowFilesystemPathError

logger = get_logger(__name__)


class WalkTask(BaseTask):
    """ Walks (recursively) down a directory and calls a callable for each file. """
    def __init__(self, name, path, callback, recursive=False, *, queue=JobType.Task,
                 callback_init=None, callback_finally=None,
                 force_run=False, propagate_skip=True):
        """ Initialize the walk task object.

        All task parameters except the name, callback, queue, force_run and propagate_skip
        can either be their native type or a callable returning the native type.

        Args:
            name (str): The name of the task.
            path (str, callable): The path to the directory that should be walked.
                                  The path has to bean an absolute path, otherwise
                                  an exception is thrown.
            callback (callable): A callable object that is called for each file in the
                                 directory given by path. The function definition is
                                 def callback(entry, data, store, signal, context).
                                 where entry is of type os.DirEntry.
            recursive (bool): Recursively look for files in the directory.
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
                                           completed: TaskState.Completed
                                           stopped: TaskState.Stopped
                                           aborted: TaskState.Aborted
                                           raised exception: TaskState.Exception
                                         data the task data, store the workflow
                                         data store, signal the task signal and
                                         context the task context.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, queue=queue,
                         callback_init=callback_init, callback_finally=callback_finally,
                         force_run=force_run, propagate_skip=propagate_skip)

        self.params = TaskParameters(path=path,
                                     recursive=recursive
                                     )
        self._callback = callback

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the walk task.

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

        if not isabs(params.path):
            raise LightflowFilesystemPathError(
                'The specified path is not an absolute path')

        for entry in self._scantree(params.path, params.recursive):
            if self._callback is not None:
                self._callback(entry, data, store, signal, context)

        return Action(data)

    def _scantree(self, path, recursive=True):
        """ (recursively) yield DirEntry objects for directory given by the path."""
        for entry in scandir(path):
            if entry.is_dir(follow_symlinks=False):
                if recursive:
                    yield from self._scantree(entry.path)
            else:
                yield entry
