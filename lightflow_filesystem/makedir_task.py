import os

from lightflow.queue import JobType
from lightflow.logger import get_logger
from lightflow.models import BaseTask, Action, TaskParameters
from .exceptions import LightflowFilesystemPathError, LightflowFilesystemMkdirError

logger = get_logger(__name__)


class MakeDirTask(BaseTask):
    """ Creates one or more new directories if they do not exist yet. """
    def __init__(self, name, paths, *, queue=JobType.Task,
                 callback_init=None, callback_finally=None,
                 force_run=False, propagate_skip=True):
        """ Initialize the MakeDir task.

        All task parameters except the name, queue, force_run and propagate_skip
        can either be their native type or a callable returning the native type.

        Args:
            name (str): The name of the task.
            paths (str/list/callable): A path, or list of paths representing the
                                       directories that should be created. The paths have
                                       to be absolute paths, otherwise an exception is
                                       thrown. This parameter can either be a string,
                                       a list of strings or a callable that returns a
                                       string or a list of strings.
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

        self.params = TaskParameters(paths=paths)

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the MakeDir task.

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
            AbsolutePathError: If the specified directories are not absolute paths.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        params = self.params.eval(data, store)
        paths = [params.paths] if isinstance(params.paths, str) else params.paths
#
        for path in paths:
            if not os.path.isabs(path):
                raise LightflowFilesystemPathError(
                    'The specified path is not an absolute path')

            if not os.path.exists(path):
                try:
                    os.makedirs(path)
                except OSError as e:
                    raise LightflowFilesystemMkdirError(e)

            else:
                logger.info('Directory {} already exists. Skip creation.'.format(path))

        return Action(data)
