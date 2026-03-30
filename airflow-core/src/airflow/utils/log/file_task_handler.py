#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""File logging handler for tasks."""

from __future__ import annotations

import heapq
import io
import logging
import os
from collections.abc import Callable, Generator, Iterator
from contextlib import suppress
from datetime import datetime
from enum import Enum
from itertools import chain, islice
from pathlib import Path
from types import GeneratorType
from typing import IO, TYPE_CHECKING, TypedDict, cast
from urllib.parse import urljoin

import pendulum
from pydantic import BaseModel, ConfigDict, ValidationError
from typing_extensions import NotRequired

from airflow.configuration import conf
from airflow.executors.executor_loader import ExecutorLoader
from airflow.utils.helpers import parse_template_string, render_template
from airflow.utils.log.log_stream_accumulator import LogStreamAccumulator
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from typing import TypeAlias

    from requests import Response

    from airflow._shared.logging.remote import (
        LogMessages,
        LogResponse,
        LogSourceInfo,
        RawLogStream,
        StreamingLogResponse,
    )
    from airflow.executors.base_executor import BaseExecutor
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancehistory import TaskInstanceHistory

CHUNK_SIZE = 1024 * 1024 * 5  # 5MB
DEFAULT_SORT_DATETIME = pendulum.datetime(2000, 1, 1)
DEFAULT_SORT_TIMESTAMP = int(DEFAULT_SORT_DATETIME.timestamp() * 1000)
SORT_KEY_OFFSET = 10000000
"""An offset used by the _create_sort_key utility.

Assuming 50 characters per line, an offset of 10,000,000 can represent approximately 500 MB of file data, which is sufficient for use as a constant.
"""
HEAP_DUMP_SIZE = 5000
HALF_HEAP_DUMP_SIZE = HEAP_DUMP_SIZE // 2

StructuredLogStream: TypeAlias = Generator["StructuredLogMessage", None, None]
"""Structured log stream, containing structured log messages."""
LogHandlerOutputStream: TypeAlias = (
    StructuredLogStream | Iterator["StructuredLogMessage"] | chain["StructuredLogMessage"]
)
"""Output stream, containing structured log messages or a chain of them."""
ParsedLog: TypeAlias = tuple[datetime | None, int, "StructuredLogMessage"]
"""Parsed log record, containing timestamp, line_num and the structured log message."""
ParsedLogStream: TypeAlias = Generator[ParsedLog, None, None]
LegacyProvidersLogType: TypeAlias = list["StructuredLogMessage"] | str | list[str]
"""Return type used by legacy `_read` methods for Alibaba Cloud, Elasticsearch, OpenSearch, and Redis log handlers.

- For Elasticsearch and OpenSearch: returns either a list of structured log messages.
- For Alibaba Cloud: returns a string.
- For Redis: returns a list of strings.
"""

_STATES_WITH_COMPLETED_ATTEMPT = frozenset(
    {
        TaskInstanceState.UP_FOR_RETRY,
        TaskInstanceState.UP_FOR_RESCHEDULE,
    }
)


logger = logging.getLogger(__name__)


class LogMetadata(TypedDict):
    """Metadata about the log fetching process, including `end_of_log` and `log_pos`."""

    end_of_log: bool
    log_pos: NotRequired[int]
    # the following attributes are used for Elasticsearch and OpenSearch log handlers
    offset: NotRequired[str | int]
    # Ensure a string here. Large offset numbers will get JSON.parsed incorrectly
    # on the client. Sending as a string prevents this issue.
    # https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/MAX_SAFE_INTEGER
    last_log_timestamp: NotRequired[str]
    max_offset: NotRequired[str]


class StructuredLogMessage(BaseModel):
    """An individual log message."""

    timestamp: datetime | None = None
    event: str

    # Collisions of sort_key may occur due to duplicated messages. If this happens, the heap will use the second element,
    # which is the StructuredLogMessage for comparison. Therefore, we need to define a comparator for it.
    def __lt__(self, other: StructuredLogMessage) -> bool:
        return self.sort_key < other.sort_key

    @property
    def sort_key(self) -> datetime:
        return self.timestamp or DEFAULT_SORT_DATETIME

    # We don't need to cache string when parsing in to this, as almost every line will have a different
    # values; `extra=allow` means we'll create extra properties as needed. Only timestamp and event are
    # required, everything else is up to what ever is producing the logs
    model_config = ConfigDict(cache_strings=False, extra="allow")


class LogType(str, Enum):
    """
    Type of service from which we retrieve logs.

    :meta private:
    """

    TRIGGER = "trigger"
    WORKER = "worker"


def _set_task_deferred_context_var():
    """
    Tell task log handler that task exited with deferral.

    This exists for the sole purpose of telling elasticsearch handler not to
    emit end_of_log mark after task deferral.

    Depending on how the task is run, we may need to set this in task command or in local task job.
    Kubernetes executor requires the local task job invocation; local executor requires the task
    command invocation.

    :meta private:
    """
    logger = logging.getLogger()
    with suppress(StopIteration):
        h = next(h for h in logger.handlers if hasattr(h, "ctx_task_deferred"))
        h.ctx_task_deferred = True


def _fetch_logs_from_service(url: str, log_relative_path: str) -> Response:
    # Import occurs in function scope for perf. Ref: https://github.com/apache/airflow/pull/21438
    import requests

    from airflow.api_fastapi.auth.tokens import JWTGenerator, get_signing_key

    timeout = conf.getint("api", "log_fetch_timeout_sec", fallback=None)
    generator = JWTGenerator(
        secret_key=get_signing_key("api", "secret_key"),
        # Since we are using a secret key, we need to be explicit about the algorithm here too
        algorithm="HS512",
        # We must set an empty private key here as otherwise it can be automatically loaded by JWTGenerator
        # and secret_key and private_key cannot be set together
        private_key=None,  # type: ignore[arg-type]
        issuer=None,
        valid_for=conf.getint("webserver", "log_request_clock_grace", fallback=30),
        audience="task-instance-logs",
    )
    response = requests.get(
        url,
        timeout=timeout,
        headers={"Authorization": generator.generate({"filename": log_relative_path})},
        stream=True,
    )
    response.encoding = "utf-8"
    return response


_parse_timestamp = conf.getimport("logging", "interleave_timestamp_parser", fallback=None)

if not _parse_timestamp:

    def _parse_timestamp(line: str):
        timestamp_str, _ = line.split(" ", 1)
        return pendulum.parse(timestamp_str.strip("[]"))


def _stream_lines_by_chunk(
    log_io: IO[str],
) -> RawLogStream:
    """
    Stream lines from a file-like IO object.

    :param log_io: A file-like IO object to read from.
    :return: A generator that yields individual lines within the specified range.
    """
    # Skip processing if file is already closed
    if log_io.closed:
        return

    # Seek to beginning if possible
    if log_io.seekable():
        try:
            log_io.seek(0)
        except Exception as e:
            logger.error("Error seeking in log stream: %s", e)
            return

    buffer = ""
    while True:
        # Check if file is already closed
        if log_io.closed:
            break

        try:
            chunk = log_io.read(CHUNK_SIZE)
        except Exception as e:
            logger.error("Error reading log stream: %s", e)
            break

        if not chunk:
            break

        buffer += chunk
        *lines, buffer = buffer.split("\n")
        yield from lines

    if buffer:
        yield from buffer.split("\n")


def _log_stream_to_parsed_log_stream(
    log_stream: RawLogStream,
) -> ParsedLogStream:
    """
    Turn a str log stream into a generator of parsed log lines.

    :param log_stream: The stream to parse.
    :return: A generator of parsed log lines.
    """
    from airflow._shared.timezones.timezone import coerce_datetime

    timestamp = None
    next_timestamp = None
    idx = 0
    for line in log_stream:
        if line:
            try:
                log = StructuredLogMessage.model_validate_json(line)
            except ValidationError:
                with suppress(Exception):
                    # If we can't parse the timestamp, don't attach one to the row
                    if isinstance(line, str):
                        next_timestamp = _parse_timestamp(line)
                log = StructuredLogMessage(event=str(line), timestamp=next_timestamp)
            if log.timestamp:
                log.timestamp = coerce_datetime(log.timestamp)
                timestamp = log.timestamp
            yield timestamp, idx, log
        idx += 1


def _create_sort_key(timestamp: datetime | None, line_num: int) -> int:
    """
    Create a sort key for log record, to be used in K-way merge.

    :param timestamp: timestamp of the log line
    :param line_num: line number of the log line
    :return: a integer as sort key to avoid overhead of memory usage
    """
    return int((timestamp or DEFAULT_SORT_DATETIME).timestamp() * 1000) * SORT_KEY_OFFSET + line_num


def _is_sort_key_with_default_timestamp(sort_key: int) -> bool:
    """
    Check if the sort key was generated with the DEFAULT_SORT_TIMESTAMP.

    This is used to identify log records that don't have timestamp.

    :param sort_key: The sort key to check
    :return: True if the sort key was generated with DEFAULT_SORT_TIMESTAMP, False otherwise
    """
    # Extract the timestamp part from the sort key (remove the line number part)
    timestamp_part = sort_key // SORT_KEY_OFFSET
    return timestamp_part == DEFAULT_SORT_TIMESTAMP


def _add_log_from_parsed_log_streams_to_heap(
    heap: list[tuple[int, StructuredLogMessage]],
    parsed_log_streams: dict[int, ParsedLogStream],
) -> None:
    """
    Add one log record from each parsed log stream to the heap, and will remove empty log stream from the dict after iterating.

    :param heap: heap to store log records
    :param parsed_log_streams: dict of parsed log streams
    """
    # We intend to initialize the list lazily, as in most cases we don't need to remove any log streams.
    # This reduces memory overhead, since this function is called repeatedly until all log streams are empty.
    log_stream_to_remove: list[int] | None = None
    for idx, log_stream in parsed_log_streams.items():
        record: ParsedLog | None = next(log_stream, None)
        if record is None:
            if log_stream_to_remove is None:
                log_stream_to_remove = []
            log_stream_to_remove.append(idx)
            continue
        timestamp, line_num, line = record
        # take int as sort key to avoid overhead of memory usage
        heapq.heappush(heap, (_create_sort_key(timestamp, line_num), line))
    # remove empty log stream from the dict
    if log_stream_to_remove is not None:
        for idx in log_stream_to_remove:
            del parsed_log_streams[idx]


def _flush_logs_out_of_heap(
    heap: list[tuple[int, StructuredLogMessage]],
    flush_size: int,
    last_log_container: list[StructuredLogMessage | None],
) -> Generator[StructuredLogMessage, None, None]:
    """
    Flush logs out of the heap, deduplicating them based on the last log.

    :param heap: heap to flush logs from
    :param flush_size: number of logs to flush
    :param last_log_container: a container to store the last log, to avoid duplicate logs
    :return: a generator that yields deduplicated logs
    """
    last_log = last_log_container[0]
    for _ in range(flush_size):
        sort_key, line = heapq.heappop(heap)
        if line != last_log or _is_sort_key_with_default_timestamp(sort_key):  # dedupe
            yield line
        last_log = line
    # update the last log container with the last log
    last_log_container[0] = last_log


def _interleave_logs(*log_streams: RawLogStream) -> StructuredLogStream:
    """
    Merge parsed log streams using K-way merge.

    By yielding HALF_CHUNK_SIZE records when heap size exceeds CHUNK_SIZE, we can reduce the chance of messing up the global order.
    Since there are multiple log streams, we can't guarantee that the records are in global order.

    e.g.

    log_stream1: ----------
    log_stream2:   ----
    log_stream3:     --------

    The first record of log_stream3 is later than the fourth record of log_stream1 !
    :param parsed_log_streams: parsed log streams
    :return: interleaved log stream
    """
    # don't need to push whole tuple into heap, which increases too much overhead
    # push only sort_key and line into heap
    heap: list[tuple[int, StructuredLogMessage]] = []
    # to allow removing empty streams while iterating, also turn the str stream into parsed log stream
    parsed_log_streams: dict[int, ParsedLogStream] = {
        idx: _log_stream_to_parsed_log_stream(log_stream) for idx, log_stream in enumerate(log_streams)
    }

    # keep adding records from logs until all logs are empty
    last_log_container: list[StructuredLogMessage | None] = [None]
    while parsed_log_streams:
        _add_log_from_parsed_log_streams_to_heap(heap, parsed_log_streams)

        # yield HALF_HEAP_DUMP_SIZE records when heap size exceeds HEAP_DUMP_SIZE
        if len(heap) >= HEAP_DUMP_SIZE:
            yield from _flush_logs_out_of_heap(heap, HALF_HEAP_DUMP_SIZE, last_log_container)

    # yield remaining records
    yield from _flush_logs_out_of_heap(heap, len(heap), last_log_container)
    # free memory
    del heap
    del parsed_log_streams


def _is_logs_stream_like(log) -> bool:
    """Check if the logs are stream-like."""
    return isinstance(log, (chain, GeneratorType))


def _get_compatible_log_stream(
    log_messages: LogMessages,
) -> RawLogStream:
    """
    Convert legacy log message blobs into a generator that yields log lines.

    :param log_messages: List of legacy log message strings.
    :return: A generator that yields interleaved log lines.
    """
    yield from chain.from_iterable(
        _stream_lines_by_chunk(io.StringIO(log_message)) for log_message in log_messages
    )


class FileTaskHandler(logging.Handler):
    """
    FileTaskHandler reads task instance logs from local or remote sources.

    It no longer handles writing (moved to Task SDK / structlog).
    """

    trigger_should_wrap = True
    inherits_from_empty_operator_log_message = (
        "Operator inherits from empty operator and thus does not have logs"
    )
    executor_instances: dict[str, BaseExecutor] = {}
    DEFAULT_EXECUTOR_KEY = "_default_executor"

    def __init__(self, base_log_folder: str):
        super().__init__()
        self.local_base = base_log_folder

        self.ctx_task_deferred = False
        """
        If true, task exited with deferral to trigger.

        Some handlers emit "end of log" markers, and may not wish to do so when task defers.
        """

    @staticmethod
    def add_triggerer_suffix(full_path, job_id=None):
        """
        Derive trigger log filename from task log filename.

        E.g. given /path/to/file.log returns /path/to/file.log.trigger.123.log, where 123
        is the triggerer id.  We use the triggerer ID instead of trigger ID to distinguish
        the files because, rarely, the same trigger could get picked up by two different
        triggerer instances.
        """
        full_path = Path(full_path).as_posix()
        full_path += f".{LogType.TRIGGER.value}"
        if job_id:
            full_path += f".{job_id}.log"
        return full_path

    @provide_session
    def _render_filename(
        self, ti: TaskInstance | TaskInstanceHistory, try_number: int, session=NEW_SESSION
    ) -> str:
        """Return the worker log filename."""
        dag_run = ti.get_dagrun(session=session)

        date = dag_run.logical_date or dag_run.run_after
        formatted_date = date.isoformat()

        template = dag_run.get_log_template(session=session).filename
        str_tpl, jinja_tpl = parse_template_string(template)
        if jinja_tpl:
            return render_template(
                jinja_tpl, {"ti": ti, "ts": formatted_date, "try_number": try_number}, native=False
            )

        if str_tpl:
            data_interval = (dag_run.data_interval_start, dag_run.data_interval_end)
            if data_interval[0]:
                data_interval_start = data_interval[0].isoformat()
            else:
                data_interval_start = ""
            if data_interval[1]:
                data_interval_end = data_interval[1].isoformat()
            else:
                data_interval_end = ""
            return str_tpl.format(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                run_id=ti.run_id,
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                logical_date=formatted_date,
                try_number=try_number,
            )
        raise RuntimeError(f"Unable to render log filename for {ti}. This should never happen")

    def _get_executor_get_task_log(
        self, ti: TaskInstance | TaskInstanceHistory
    ) -> Callable[[TaskInstance | TaskInstanceHistory, int], tuple[list[str], list[str]]]:
        """
        Get the get_task_log method from executor of current task instance.

        Since there might be multiple executors, so we need to get the executor of current task instance instead of getting from default executor.

        :param ti: task instance object
        :return: get_task_log method of the executor
        """
        executor_name = ti.executor or self.DEFAULT_EXECUTOR_KEY
        executor = self.executor_instances.get(executor_name)
        if executor is not None:
            return executor.get_task_log

        if executor_name == self.DEFAULT_EXECUTOR_KEY:
            self.executor_instances[executor_name] = ExecutorLoader.get_default_executor()
        else:
            self.executor_instances[executor_name] = ExecutorLoader.load_executor(executor_name)
        return self.executor_instances[executor_name].get_task_log

    def _read(
        self,
        ti: TaskInstance | TaskInstanceHistory,
        try_number: int,
        metadata: LogMetadata | None = None,
    ) -> tuple[LogHandlerOutputStream | LegacyProvidersLogType, LogMetadata]:
        """
        Template method that contains custom logic of reading logs given the try_number.

        :param ti: task instance record
        :param try_number: current try_number to read log from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
                         Following attributes are used:
                         log_pos: (absolute) Char position to which the log
                                  which was retrieved in previous calls, this
                                  part will be skipped and only following test
                                  returned to be added to tail.
        :return: log message as a string and metadata.
                 Following attributes are used in metadata:
                 end_of_log: Boolean, True if end of log is reached or False
                             if further calls might get more log text.
                             This is determined by the status of the TaskInstance
                 log_pos: (absolute) Char position to which the log is retrieved
        """
        # Task instance here might be different from task instance when
        # initializing the handler. Thus explicitly getting log location
        # is needed to get correct log path.
        worker_log_rel_path = self._render_filename(ti, try_number)
        sources: LogSourceInfo = []
        source_list: list[str] = []
        remote_logs: list[RawLogStream] = []
        local_logs: list[RawLogStream] = []
        executor_logs: list[RawLogStream] = []
        served_logs: list[RawLogStream] = []
        with suppress(NotImplementedError):
            sources, logs = self._read_remote_logs(ti, try_number, metadata)
            if not logs:
                remote_logs = []
            elif isinstance(logs, list) and isinstance(logs[0], str):
                # If the logs are in legacy format, convert them to a generator of log lines
                remote_logs = [
                    # We don't need to use the log_pos here, as we are using the metadata to track the position
                    _get_compatible_log_stream(cast("list[str]", logs))
                ]
            elif isinstance(logs, list) and _is_logs_stream_like(logs[0]):
                # If the logs are already in a stream-like format, we can use them directly
                remote_logs = cast("list[RawLogStream]", logs)
            else:
                # If the logs are in a different format, raise an error
                raise TypeError("Logs should be either a list of strings or a generator of log lines.")
            # Extend LogSourceInfo
            source_list.extend(sources)
        has_k8s_exec_pod = False
        if ti.state == TaskInstanceState.RUNNING:
            executor_get_task_log = self._get_executor_get_task_log(ti)
            response = executor_get_task_log(ti, try_number)
            if response:
                sources, logs = response
                # make the logs stream-like compatible
                executor_logs = [_get_compatible_log_stream(logs)]
            if sources:
                source_list.extend(sources)
                has_k8s_exec_pod = True
        if not (remote_logs and ti.state not in State.unfinished):
            # when finished, if we have remote logs, no need to check local
            worker_log_full_path = Path(self.local_base, worker_log_rel_path)
            sources, local_logs = self._read_from_local(worker_log_full_path)
            source_list.extend(sources)
        if ti.state in (TaskInstanceState.RUNNING, TaskInstanceState.DEFERRED) and not has_k8s_exec_pod:
            sources, served_logs = self._read_from_logs_server(ti, worker_log_rel_path)
            source_list.extend(sources)
        elif (ti.state not in State.unfinished or ti.state in _STATES_WITH_COMPLETED_ATTEMPT) and not (
            local_logs or remote_logs
        ):
            # ordinarily we don't check served logs, with the assumption that users set up
            # remote logging or shared drive for logs for persistence, but that's not always true
            # so even if task is done, if no local logs or remote logs are found, we'll check the worker
            sources, served_logs = self._read_from_logs_server(ti, worker_log_rel_path)
            source_list.extend(sources)

        out_stream: LogHandlerOutputStream = _interleave_logs(
            *local_logs,
            *remote_logs,
            *executor_logs,
            *served_logs,
        )

        # Log message source details are grouped: they are not relevant for most users and can
        # distract them from finding the root cause of their errors
        header = [
            StructuredLogMessage(event="::group::Log message source details", sources=source_list),  # type: ignore[call-arg]
            StructuredLogMessage(event="::endgroup::"),
        ]
        end_of_log = ti.try_number != try_number or ti.state not in (
            TaskInstanceState.RUNNING,
            TaskInstanceState.DEFERRED,
        )

        with LogStreamAccumulator(out_stream, HEAP_DUMP_SIZE) as stream_accumulator:
            log_pos = stream_accumulator.total_lines
            out_stream = stream_accumulator.stream

            # skip log stream until the last position
            if metadata and "log_pos" in metadata:
                out_stream = islice(out_stream, metadata["log_pos"], None)
            else:
                # first time reading log, add messages before interleaved log stream
                out_stream = chain(header, out_stream)

            return out_stream, {
                "end_of_log": end_of_log,
                "log_pos": log_pos,
            }

    @staticmethod
    @staticmethod
    def _get_pod_namespace(ti: TaskInstance | TaskInstanceHistory):
        pod_override = getattr(ti.executor_config, "pod_override", None)
        metadata = getattr(pod_override, "metadata", None)
        namespace = None
        with suppress(Exception):
            namespace = getattr(metadata, "namespace", None)
        return namespace or conf.get("kubernetes_executor", "namespace")

    def _get_log_retrieval_url(
        self,
        ti: TaskInstance | TaskInstanceHistory,
        log_relative_path: str,
        log_type: LogType | None = None,
    ) -> tuple[str, str]:
        """Given TI, generate URL with which to fetch logs from service log server."""
        if log_type == LogType.TRIGGER:
            if not ti.triggerer_job:
                raise RuntimeError("Could not build triggerer log URL; no triggerer job.")
            config_key = "triggerer_log_server_port"
            config_default = 8794
            hostname = ti.triggerer_job.hostname
            log_relative_path = self.add_triggerer_suffix(log_relative_path, job_id=ti.triggerer_job.id)
        else:
            hostname = ti.hostname
            config_key = "worker_log_server_port"
            config_default = 8793
        return (
            urljoin(
                f"http://{hostname}:{conf.get('logging', config_key, fallback=config_default)}/log/",
                log_relative_path,
            ),
            log_relative_path,
        )

    def read(
        self,
        task_instance: TaskInstance | TaskInstanceHistory,
        try_number: int | None = None,
        metadata: LogMetadata | None = None,
    ) -> tuple[LogHandlerOutputStream, LogMetadata]:
        """
        Read logs of given task instance from local machine.

        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from. If None
                            it returns the log of task_instance.try_number
        :param metadata: log metadata, can be used for steaming log reading and auto-tailing.
        :return: a list of listed tuples which order log string by host
        """
        if try_number is None:
            try_number = task_instance.try_number

        if try_number == 0 and task_instance.state in (
            TaskInstanceState.SKIPPED,
            TaskInstanceState.UPSTREAM_FAILED,
        ):
            logs = [StructuredLogMessage(event="Task was skipped, no logs available.")]
            return chain(logs), {"end_of_log": True}

        if try_number is None or try_number < 1:
            logs = [
                StructuredLogMessage(  # type: ignore[call-arg]
                    level="error", event=f"Error fetching the logs. Try number {try_number} is invalid."
                )
            ]
            return chain(logs), {"end_of_log": True}

        # compatibility for es_task_handler and os_task_handler
        read_result = self._read(task_instance, try_number, metadata)
        out_stream, metadata = read_result
        # If the out_stream is None or empty, return the read result
        if not out_stream:
            out_stream = cast("Generator[StructuredLogMessage, None, None]", out_stream)
            return out_stream, metadata

        if _is_logs_stream_like(out_stream):
            out_stream = cast("Generator[StructuredLogMessage, None, None]", out_stream)
            return out_stream, metadata
        if isinstance(out_stream, list) and isinstance(out_stream[0], StructuredLogMessage):
            out_stream = cast("list[StructuredLogMessage]", out_stream)
            return (log for log in out_stream), metadata
        if isinstance(out_stream, list) and isinstance(out_stream[0], str):
            # If the out_stream is a list of strings, convert it to a generator
            out_stream = cast("list[str]", out_stream)
            raw_stream = _stream_lines_by_chunk(io.StringIO("".join(out_stream)))
            out_stream = (log for _, _, log in _log_stream_to_parsed_log_stream(raw_stream))
            return out_stream, metadata
        if isinstance(out_stream, str):
            # If the out_stream is a string, convert it to a generator
            raw_stream = _stream_lines_by_chunk(io.StringIO(out_stream))
            out_stream = (log for _, _, log in _log_stream_to_parsed_log_stream(raw_stream))
            return out_stream, metadata
        raise TypeError(
            "Invalid log stream type. Expected a generator of StructuredLogMessage, list of StructuredLogMessage, list of str or str."
            f" Got {type(out_stream).__name__} instead."
            f" Content type: {type(out_stream[0]).__name__ if isinstance(out_stream, (list, tuple)) and out_stream else 'empty'}"
        )

    @staticmethod
    def _read_from_local(
        worker_log_path: Path,
    ) -> StreamingLogResponse:
        sources: LogSourceInfo = []
        log_streams: list[RawLogStream] = []
        paths = sorted(worker_log_path.parent.glob(worker_log_path.name + "*"))
        if not paths:
            return sources, log_streams

        for path in paths:
            sources.append(os.fspath(path))
            # Read the log file and yield lines
            log_streams.append(_stream_lines_by_chunk(open(path, encoding="utf-8")))
        return sources, log_streams

    def _read_from_logs_server(
        self,
        ti: TaskInstance | TaskInstanceHistory,
        worker_log_rel_path: str,
    ) -> StreamingLogResponse:
        sources: LogSourceInfo = []
        log_streams: list[RawLogStream] = []
        try:
            log_type = LogType.TRIGGER if getattr(ti, "triggerer_job", False) else LogType.WORKER
            url, rel_path = self._get_log_retrieval_url(ti, worker_log_rel_path, log_type=log_type)
            response = _fetch_logs_from_service(url, rel_path)
            if response.status_code == 403:
                sources.append(
                    "!!!! Please make sure that all your Airflow components (e.g. "
                    "schedulers, api-servers, dag-processors, workers and triggerer) have "
                    "the same 'secret_key' configured in '[api]' section and "
                    "time is synchronized on all your machines (for example with ntpd)\n"
                    "See more at https://airflow.apache.org/docs/apache-airflow/"
                    "stable/configurations-ref.html#secret-key"
                )
            elif response.status_code == 404:
                # Log file not found on the worker's log server.
                # This typically happens when a task retried on a different worker
                # and the original worker's logs are no longer accessible.
                # Fall back to local filesystem read if available.
                worker_log_full_path = Path(self.local_base, worker_log_rel_path)
                fallback_sources, fallback_streams = self._read_from_local(worker_log_full_path)
                if fallback_sources:
                    sources.extend(fallback_sources)
                    log_streams.extend(fallback_streams)
                else:
                    sources.append(
                        f"Log file not found on worker '{ti.hostname}'. "
                        f"This attempt may have run on a different worker whose logs "
                        f"are no longer accessible. "
                        f"Consider configuring remote logging (S3, GCS, etc.) for log persistence."
                    )
            else:
                # Check if the resource was properly fetched
                response.raise_for_status()

                if int(response.headers.get("Content-Length", 0)) > 0:
                    sources.append(url)
                    log_streams.append(
                        _stream_lines_by_chunk(io.TextIOWrapper(cast("IO[bytes]", response.raw)))
                    )
        except Exception as e:
            from requests.exceptions import InvalidURL

            if (
                isinstance(e, InvalidURL)
                and ti.task is not None
                and ti.task.inherits_from_empty_operator is True
            ):
                sources.append(self.inherits_from_empty_operator_log_message)
            else:
                sources.append(f"Could not read served logs: {e}")
                logger.exception("Could not read served logs")
        return sources, log_streams

    def _read_remote_logs(self, ti, try_number, metadata=None) -> LogResponse | StreamingLogResponse:
        """
        Implement in subclasses to read from the remote service.

        This method should return two lists, messages and logs.

        * Each element in the messages list should be a single message,
          such as, "reading from x file".
        * Each element in the logs list should be the content of one file.
        """
        remote_io = None
        try:
            from airflow.logging_config import get_remote_task_log

            remote_io = get_remote_task_log()
        except Exception:
            pass

        if remote_io is None:
            # Import not found, or explicitly set to None
            raise NotImplementedError

        # This living here is not really a good plan, but it just about works for now.
        # Ideally we move all the read+combine logic in to TaskLogReader and out of the task handler.
        path = self._render_filename(ti, try_number)
        if stream_method := getattr(remote_io, "stream", None):
            # Use .stream interface if provider's RemoteIO supports it
            sources, logs = stream_method(path, ti)
            return sources, logs or []
        # Fallback to .read interface
        sources, logs = remote_io.read(path, ti)
        return sources, logs or []
