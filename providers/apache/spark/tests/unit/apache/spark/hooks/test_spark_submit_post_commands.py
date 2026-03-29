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
"""
Tests for the post_submit_commands feature added to SparkSubmitHook.
Issue: https://github.com/apache/airflow/issues/50958
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook


def _make_hook(post_submit_commands=None, **extra):
    """Build a SparkSubmitHook with a mocked connection."""

    with (
        patch.object(SparkSubmitHook, "_resolve_connection") as mock_conn,
        patch.object(SparkSubmitHook, "_resolve_should_track_driver_status", return_value=False),
    ):
        mock_conn.return_value = {
            "master": "local",
            "queue": None,
            "deploy_mode": "client",
            "spark_binary": "spark-submit",
            "namespace": None,
            "principal": None,
            "keytab": None,
        }
        hook = SparkSubmitHook(
            conn_id="spark_default",
            post_submit_commands=post_submit_commands,
            **extra,
        )
    return hook


class TestNoPostSubmitCommands:
    def test_defaults_to_empty_list(self):
        hook = _make_hook()
        assert hook._post_submit_commands == []

    def test_run_post_submit_commands_is_noop_when_empty(self):
        hook = _make_hook()
        with patch("subprocess.run") as mock_run:
            hook._run_post_submit_commands()
            mock_run.assert_not_called()


class TestSingleCommand:
    def test_command_is_stored(self):
        hook = _make_hook(post_submit_commands=["echo hello"])
        assert hook._post_submit_commands == ["echo hello"]

    def test_command_is_executed_via_shell(self):
        hook = _make_hook(post_submit_commands=["echo hello"])
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "hello\n"

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            hook._run_post_submit_commands()
            mock_run.assert_called_once_with(
                "echo hello",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
                timeout=30,
            )

    def test_istio_quit_command(self):
        istio_cmd = "curl -X POST localhost:15020/quitquitquit"
        hook = _make_hook(post_submit_commands=[istio_cmd])
        mock_result = MagicMock(returncode=0, stdout="")

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            hook._run_post_submit_commands()
            mock_run.assert_called_once()
            args, kwargs = mock_run.call_args
            assert args[0] == istio_cmd
            assert kwargs["shell"] is True


class TestMultipleCommands:
    def test_multiple_commands_executed_in_order(self):
        cmds = [
            "curl -X POST localhost:15020/quitquitquit",
            "echo cleanup done",
            "rm -f /tmp/spark.lock",
        ]
        hook = _make_hook(post_submit_commands=cmds)
        mock_result = MagicMock(returncode=0, stdout="")

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            hook._run_post_submit_commands()
            assert mock_run.call_count == 3
            actual_cmds = [c.args[0] for c in mock_run.call_args_list]
            assert actual_cmds == cmds


class TestFailureResilience:
    def test_nonzero_exit_does_not_raise(self):
        hook = _make_hook(post_submit_commands=["bad-command"])
        mock_result = MagicMock(returncode=127, stdout="command not found\n")
        with patch("subprocess.run", return_value=mock_result):
            hook._run_post_submit_commands()

    def test_timeout_does_not_raise(self):
        hook = _make_hook(post_submit_commands=["sleep 999"])
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("sleep 999", 30)):
            hook._run_post_submit_commands()

    def test_exception_does_not_raise(self):
        hook = _make_hook(post_submit_commands=["anything"])
        with patch("subprocess.run", side_effect=OSError("no such file")):
            hook._run_post_submit_commands()

    def test_remaining_commands_run_after_failure(self):
        cmds = ["bad-command", "echo still-runs"]
        hook = _make_hook(post_submit_commands=cmds)
        results = [
            MagicMock(returncode=1, stdout="error"),
            MagicMock(returncode=0, stdout="still-runs"),
        ]
        with patch("subprocess.run", side_effect=results) as mock_run:
            hook._run_post_submit_commands()
            assert mock_run.call_count == 2


class TestSubmitIntegration:
    def test_post_commands_called_after_submit(self):
        hook = _make_hook(post_submit_commands=["echo post"])
        hook._is_yarn = False
        hook._is_kubernetes = False
        hook._should_track_driver_status = False

        mock_proc = MagicMock()
        mock_proc.stdout = iter(["log line 1\n"])
        mock_proc.wait.return_value = 0

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            patch.object(hook, "_run_post_submit_commands") as mock_post,
        ):
            hook.submit("my_app.py")
            mock_post.assert_called_once()

    def test_post_commands_not_called_when_submit_fails(self):
        hook = _make_hook(post_submit_commands=["echo should-not-run"])
        hook._is_yarn = False
        hook._is_kubernetes = False
        hook._should_track_driver_status = False

        mock_proc = MagicMock()
        mock_proc.stdout = iter([])
        mock_proc.wait.return_value = 1

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            patch.object(hook, "_run_post_submit_commands") as mock_post,
        ):
            with pytest.raises(AirflowException):
                hook.submit("my_app.py")
            mock_post.assert_not_called()


class TestOnKillIntegration:
    def test_post_commands_called_after_on_kill(self):
        hook = _make_hook(post_submit_commands=["echo killed-cleanup"])
        hook._should_track_driver_status = False
        hook._submit_sp = None
        hook._yarn_application_id = None
        hook._kubernetes_driver_pod = None

        with patch.object(hook, "_run_post_submit_commands") as mock_post:
            hook.on_kill()
            mock_post.assert_called_once()


class TestBackwardCompatibility:
    def test_hook_without_param_still_works(self):
        hook = _make_hook()
        assert hook._post_submit_commands == []

    def test_none_is_treated_as_empty(self):
        hook = _make_hook(post_submit_commands=None)
        assert hook._post_submit_commands == []
