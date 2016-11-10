import os
import time

from lightflow.logger import get_logger
from lightflow.tasks import TriggerTask
from lightflow.models import TaskParameters
from .exceptions import LightflowFilesystemPathError


logger = get_logger(__name__)


class NewLineTriggerTask(TriggerTask):
    """ Triggers the execution of a DAG upon a new line added to a file.

    This trigger task watches a specified file for new line. After having
    aggregated a given number of lines changes it sends a signal to the parent workflow to
    execute the specified DAG. A list of lines that were added is given to the
    DAG prior to its execution.
    """
    def __init__(self, name, dag_name, path,
                 out_key=None, aggregate=None,
                 use_existing=False, flush_existing=True,
                 event_trigger_time=0.5, stop_polling_rate=2,
                 force_run=False, propagate_skip=True):
        """ Initialise the filesystem notify trigger task.

        All task parameters except the name, force_run and propagate_skip can either be
        their native type or a callable returning the native type.

        Args:
            name (str): The name of the task.
            dag_name: The name of the DAG that should be executed after the
                      specified number of file change events has occurred.
            path: The path to the file that should be watched for new lines.
                  The path has to be an absolute path, otherwise an exception is thrown.
            out_key: The key under which the list of lines is being stored in the
                      data that is passed to the DAG. The default is 'lines'.
            aggregate: The number of lines that are aggregated before the DAG
                       is triggered. Set to None or 1 to trigger on each new line
                       event occurrence.
            use_existing: Use the existing lines that are located in file for
                          initialising the line list.
            flush_existing: If use_existing is True, then flush all existing lines without
                            regard to the aggregation setting. I.e,. all existing lines
                            saved to data and sent to target DAG.
            event_trigger_time: The waiting time between events in seconds. Set
                                to None to turn off.
            stop_polling_rate: The number of events after which a signal is sent
                               to the workflow to check whether the task
                               should be stopped.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, dag_name, force_run, propagate_skip)

        # set the tasks's parameters
        self.params = TaskParameters(
            path=path,
            out_key=out_key if out_key is not None else 'lines',
            aggregate=aggregate if aggregate is not None else 1,
            use_existing=use_existing,
            flush_existing=flush_existing,
            event_trigger_time=event_trigger_time,
            stop_polling_rate=stop_polling_rate,
        )


    def run(self, data, data_store, signal, **kwargs):
        """ The main run method of the NotifyTriggerTask task.

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

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        params = self.params.eval(data, data_store)

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
                data[params.out_key] = lines
                signal.run_dag(self._dag_name, data=data)
                del lines[:]

        polling_event_number = 0

        def watch_file(file_pointer):
            while True:
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

            for line in watch_file(file):
                lines.append(line)

                # check every stop_polling_rate events the stop signal
                polling_event_number += 1
                if polling_event_number > params.stop_polling_rate:
                    polling_event_number = 0
                    if signal.is_stopped():
                        break

                # as soon as enough line have been aggregated call the sub dag
                if len(lines) >= params.aggregate:
                    chunks = len(lines) // params.aggregate
                    for i in range(0, chunks):
                        data[params.out_key] = lines[0:params.aggregate]
                        signal.run_dag(self._dag_name, data=data)
                        del lines[0:params.aggregate]
        finally:
            file.close()
