"""Sparkflow Python client for interacting with the Sparkflow server API."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError


class SparkflowClient:
    """HTTP client for the Sparkflow REST API.

    Example:
        client = SparkflowClient("http://localhost:8080")
        dags = client.list_dags()
        run_id = client.trigger_dag("my-dag", params={"date": "2024-01-01"})
        status = client.get_run(run_id)
    """

    def __init__(self, base_url: str = "http://localhost:8080", token: str = ""):
        self.base_url = base_url.rstrip("/")
        self.token = token

    def _request(
        self,
        method: str,
        path: str,
        data: Optional[dict] = None,
    ) -> dict:
        url = f"{self.base_url}{path}"
        body = json.dumps(data).encode() if data else None

        req = Request(url, data=body, method=method)
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")

        try:
            with urlopen(req) as resp:
                response_data = resp.read().decode()
                if response_data:
                    return json.loads(response_data)
                return {}
        except URLError as e:
            raise ConnectionError(f"Failed to connect to Sparkflow: {e}") from e

    def health(self) -> dict:
        """Check server health."""
        return self._request("GET", "/healthz")

    def list_dags(self, limit: int = 100) -> List[dict]:
        """List all DAGs."""
        result = self._request("GET", f"/api/v1/dags?limit={limit}")
        return result.get("dags", [])

    def get_dag(self, dag_id: str) -> dict:
        """Get a specific DAG."""
        return self._request("GET", f"/api/v1/dags/{dag_id}")

    def create_dag(self, yaml_content: str) -> dict:
        """Create a DAG from YAML."""
        return self._request("POST", "/api/v1/dags", {"yaml_content": yaml_content})

    def delete_dag(self, dag_id: str) -> dict:
        """Delete a DAG."""
        return self._request("DELETE", f"/api/v1/dags/{dag_id}")

    def trigger_dag(self, dag_id: str, params: Optional[Dict[str, str]] = None) -> str:
        """Trigger a DAG run and return the run ID."""
        result = self._request("POST", f"/api/v1/dags/{dag_id}/trigger", {
            "params": params or {},
        })
        return result.get("run_id", "")

    def get_run(self, run_id: str) -> dict:
        """Get a specific DAG run."""
        return self._request("GET", f"/api/v1/runs/{run_id}")

    def list_runs(
        self,
        dag_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[dict]:
        """List DAG runs."""
        path = "/api/v1/runs"
        params = [f"limit={limit}"]
        if dag_id:
            params.append(f"dag_id={dag_id}")
        if params:
            path += "?" + "&".join(params)
        result = self._request("GET", path)
        return result.get("runs", [])

    def cancel_run(self, run_id: str) -> dict:
        """Cancel a running DAG."""
        return self._request("POST", f"/api/v1/runs/{run_id}/cancel")

    def get_task_logs(self, task_instance_id: str) -> str:
        """Get logs for a task instance."""
        result = self._request("GET", f"/api/v1/tasks/{task_instance_id}/logs")
        return result.get("logs", "")

    def retry_task(self, task_instance_id: str) -> dict:
        """Retry a failed task."""
        return self._request("POST", f"/api/v1/tasks/{task_instance_id}/retry")

    def get_workers(self) -> List[dict]:
        """List all workers."""
        result = self._request("GET", "/api/v1/admin/workers")
        return result.get("workers", [])

    def get_dlq(self, limit: int = 100) -> List[dict]:
        """Get dead letter queue entries."""
        result = self._request("GET", f"/api/v1/admin/dlq?limit={limit}")
        return result.get("entries", [])
