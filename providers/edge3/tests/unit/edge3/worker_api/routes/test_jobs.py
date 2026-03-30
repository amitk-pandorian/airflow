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
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from airflow.executors.workloads import BundleInfo, ExecuteTask
from airflow.providers.edge3.models.edge_job import EdgeJobModel
from airflow.providers.edge3.worker_api.routes.jobs import parse_command, state
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.executors.workloads import ExecuteCallback

if AIRFLOW_V_3_2_PLUS:
    from airflow.executors.workloads import CallbackFetchMethod, ExecuteCallback, TaskInstanceDTO
    from airflow.executors.workloads.callback import CallbackDTO


try:
    from airflow.sdk._shared.observability.metrics.dual_stats_manager import DualStatsManager  # noqa: F401

    stats_reference = "airflow.sdk._shared.observability.metrics.dual_stats_manager.DualStatsManager"
    expected_call_count = 1
except ImportError:
    from airflow.providers.common.compat.sdk import Stats

    stats_reference = f"{Stats.__module__}.Stats"
    expected_call_count = 2

pytestmark = pytest.mark.db_test


DAG_ID = "my_dag"
TASK_ID = "my_task"
RUN_ID = "manual__2024-11-24T21:03:01+01:00"
QUEUE = "test"


class TestJobsApiRoutes:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, dag_maker, session: Session):
        session.execute(delete(EdgeJobModel))
        session.commit()

    @patch(f"{stats_reference}.incr")
    def test_state(self, mock_stats_incr, session: Session):
        with create_session() as session:
            job = EdgeJobModel(
                dag_id=DAG_ID,
                task_id=TASK_ID,
                run_id=RUN_ID,
                try_number=1,
                map_index=-1,
                state=TaskInstanceState.RUNNING,
                queue=QUEUE,
                concurrency_slots=1,
                command="execute",
            )
            session.add(job)
            session.commit()
            state(
                dag_id=DAG_ID,
                task_id=TASK_ID,
                run_id=RUN_ID,
                try_number=1,
                map_index=-1,
                state=TaskInstanceState.RUNNING,
                session=session,
            )

            mock_stats_incr.assert_not_called()

            state(
                dag_id=DAG_ID,
                task_id=TASK_ID,
                run_id=RUN_ID,
                try_number=1,
                map_index=-1,
                state=TaskInstanceState.SUCCESS,
                session=session,
            )

            mock_stats_incr.assert_called_with(
                "edge_worker.ti.finish",
                tags={
                    "dag_id": DAG_ID,
                    "queue": QUEUE,
                    "state": TaskInstanceState.SUCCESS,
                    "task_id": TASK_ID,
                },
            )
            assert mock_stats_incr.call_count == expected_call_count

            db_job: EdgeJobModel | None = session.scalar(select(EdgeJobModel))
            assert db_job is not None
            assert db_job.state == TaskInstanceState.SUCCESS


@pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="The tests should be skipped for Airflow < 3.2")
class TestParseCommand:
    def _make_execute_task(self) -> ExecuteTask:
        ti = TaskInstanceDTO(
            id=uuid4(),
            dag_version_id=uuid4(),
            task_id="test_task",
            dag_id="test_dag",
            run_id="test_run",
            try_number=1,
            map_index=-1,
            pool_slots=1,
            queue="default",
            priority_weight=1,
        )
        return ExecuteTask(
            ti=ti,
            dag_rel_path=Path("test_dag.py"),
            token="test_token",
            bundle_info=BundleInfo(name="test_bundle", version="1.0"),
            log_path="test.log",
        )

    def _make_execute_callback(self) -> ExecuteCallback:
        callback_data = CallbackDTO(
            id="12345678-1234-5678-1234-567812345678",
            fetch_method=CallbackFetchMethod.IMPORT_PATH,
            data={
                "path": "builtins.dict",
                "kwargs": {"a": 1, "b": 2, "c": 3},
            },
        )
        return ExecuteCallback(
            callback=callback_data,
            dag_rel_path=Path("test.py"),
            bundle_info=BundleInfo(name="test_bundle", version="1.0"),
            token="test_token",
            log_path="test.log",
        )

    def test_parse_command_execute_task(self):
        workload = self._make_execute_task()
        command_json = workload.model_dump_json()

        result = parse_command(command_json, dag_id="test_dag", run_id="test_run")

        assert isinstance(result, ExecuteTask)
        assert result.ti.dag_id == "test_dag"
        assert result.ti.task_id == "test_task"

    def test_parse_command_execute_callback(self):
        workload = self._make_execute_callback()
        command_json = workload.model_dump_json()
        # Mimic how edge_executor stores callback jobs
        dag_id = ExecuteCallback.TYPE
        run_id = f"ExecuteCallback-{workload.callback.key}"

        result = parse_command(command_json, dag_id=dag_id, run_id=run_id)

        assert isinstance(result, ExecuteCallback)
        assert result.callback.id == "12345678-1234-5678-1234-567812345678"
        assert result.callback.fetch_method == CallbackFetchMethod.IMPORT_PATH

    def test_parse_command_non_callback_dag_id_returns_execute_task(self):
        """Even if run_id starts with ExecuteCallback, dag_id must also match."""
        workload = self._make_execute_task()
        command_json = workload.model_dump_json()

        result = parse_command(command_json, dag_id="some_dag", run_id="ExecuteCallback-something")

        assert isinstance(result, ExecuteTask)
