import os
import time

from lightflow.queue import JobType
from lightflow.logger import get_logger
from lightflow.models import BaseTask, TaskParameters, Action
from .exceptions import LightflowFilesystemPathError


logger = get_logger(__name__)


class NewLineTriggerTask(BaseTask):
    """ Triggers a callback function upon a new line added to a file.

    This trigger task watches a specified file for new line. After having
    aggregated a given number of line changes it calls the provided callback function with
    a list of lines that were added.
    """
    def __init__(self, name, path, callback,
                 aggregate=None, use_existing=False, flush_existing=True,
                 event_trigger_time=0.5, stop_polling_rate=2, *,
                 callback_init=None, callback_finally=None,
                 queue=JobType.Task, force_run=False, propagate_skip=True):
        """ Initialize the filesystem notify trigger task.

        All task parameters except the name, callback, queue, force_run and propagate_skip
        can either be their native type or a callable returning the native type.

        Args:
            name (str): The name of the task.
            path: The path to the file that should be watched for new lines.
                  The path has to be an absolute path, otherwise an exception is thrown.
            callback (callable): A callable object that is called with the list of lines
                                 that have changed. The function definition is
                                 def callback(lines, data, store, signal, context).
            aggregate (int, None): The number of lines that are aggregated before the
                                   callback is called. Set to None or 1 to trigger
                                   on each new line event occurrence.
            use_existing (bool): Use the existing lines that are located in file for
                                 initialising the line list.
            flush_existing (bool): If 'use_existing' is True, then flush all existing
                                   lines without regard to the aggregation setting.
                                   I.e,. all existing lines are sent to the callback.
            event_trigger_time (float, None): The waiting time between events in seconds.
                                              Set to None to turn off.
            stop_polling_rate (float): The number of events after which a signal is sent
                                       to the workflow to check whether the task
                                       should be stopped.
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

        # set the tasks's parameters
        self.params = TaskParameters(
            path=path,
            aggregate=aggregate if aggregate is not None else 1,
            use_existing=use_existing,
            flush_existing=flush_existing,
            event_trigger_time=event_trigger_time,
            stop_polling_rate=stop_polling_rate,
        )
        self._callback = callback

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the NotifyTriggerTask task.

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
        """
        params = self.params.eval(data, store)

        if not os.path.isabs(params.path):
            raise LightflowFilesystemPathError(
                'The specified path is not an absolute path')

        # if requested, pre-fill the file list with existing lines
        lines = []
        num_read_lines = 0
        if params.use_existing:
            with open(params.path, 'r') as file:
                lines = file.readlines()

            num_read_lines = len(lines)
            if params.flush_existing and num_read_lines > 0:
                if self._callback is not None:
                    self._callback(lines, data, store, signal, context)

                del lines[:]

        polling_event_number = 0

        def watch_file(file_pointer, task_signal):
            while True:
                if task_signal.is_stopped:
                    break

                new = file_pointer.readline()
                if new:
                    yield new
                else:
                    time.sleep(params.event_trigger_time)

        file = open(params.path, 'r')
        try:
            if params.use_existing:
                for i in range(num_read_lines):
                    file.readline()
            else:
                file.seek(0, 2)

            for line in watch_file(file, signal):
                lines.append(line)

                # check every stop_polling_rate events the stop signal
                polling_event_number += 1
                if polling_event_number > params.stop_polling_rate:
                    polling_event_number = 0
                    if signal.is_stopped:
                        break

                # as soon as enough lines have been aggregated call the callback function
                if len(lines) >= params.aggregate:
                    chunks = len(lines) // params.aggregate
                    for i in range(0, chunks):
                        if self._callback is not None:
                            self._callback(lines[0:params.aggregate], data,
                                           store, signal, context)

                        del lines[0:params.aggregate]
        finally:
            file.close()

        return Action(data)
