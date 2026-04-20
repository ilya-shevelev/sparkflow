"""DAG builder with decorator support for defining workflows in Python."""

from __future__ import annotations

import functools
import inspect
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import yaml


@dataclass
class RetryPolicy:
    """Retry policy configuration."""
    max_retries: int = 3
    initial_backoff: str = "10s"
    max_backoff: str = "5m"
    backoff_factor: float = 2.0

    def to_dict(self) -> dict:
        return {
            "max_retries": self.max_retries,
            "initial_backoff": self.initial_backoff,
            "max_backoff": self.max_backoff,
            "backoff_factor": self.backoff_factor,
        }


@dataclass
class TaskDefinition:
    """A task within a DAG."""
    id: str
    name: str
    executor: str
    config: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    timeout: Optional[str] = None
    retry_policy: Optional[RetryPolicy] = None
    priority: int = 0
    labels: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "name": self.name,
            "executor": self.executor,
            "config": self.config,
        }
        if self.dependencies:
            d["dependencies"] = self.dependencies
        if self.timeout:
            d["timeout"] = self.timeout
        if self.retry_policy:
            d["retry_policy"] = self.retry_policy.to_dict()
        if self.priority:
            d["priority"] = self.priority
        if self.labels:
            d["labels"] = self.labels
        return d


@dataclass
class CronSchedule:
    """Cron schedule configuration."""
    expression: str
    timezone: str = "UTC"
    catchup: bool = False

    def to_dict(self) -> dict:
        return {
            "expression": self.expression,
            "timezone": self.timezone,
            "catchup": self.catchup,
        }


class DAGBuilder:
    """Builds a DAG definition using a fluent API.

    Example:
        builder = DAGBuilder("my-dag", "My DAG")
        builder.schedule("0 * * * *")
        builder.add_task(TaskDefinition(
            id="task1",
            name="Task 1",
            executor="shell",
            config={"command": "echo hello"},
        ))
        yaml_str = builder.to_yaml()
    """

    def __init__(self, dag_id: str, name: str, description: str = ""):
        self._id = dag_id
        self._name = name
        self._description = description
        self._tasks: List[TaskDefinition] = []
        self._schedule: Optional[CronSchedule] = None
        self._config: Dict[str, Any] = {}
        self._max_concurrency: int = 16
        self._timeout: Optional[str] = None
        self._owner: str = ""
        self._tags: List[str] = []
        self._params: Dict[str, str] = {}
        self._default_retry: Optional[RetryPolicy] = None

    def schedule(self, expression: str, timezone: str = "UTC", catchup: bool = False) -> DAGBuilder:
        """Set the cron schedule."""
        self._schedule = CronSchedule(expression, timezone, catchup)
        return self

    def max_concurrency(self, n: int) -> DAGBuilder:
        """Set maximum task concurrency."""
        self._max_concurrency = n
        return self

    def timeout(self, duration: str) -> DAGBuilder:
        """Set DAG-level timeout."""
        self._timeout = duration
        return self

    def owner(self, owner: str) -> DAGBuilder:
        """Set the DAG owner."""
        self._owner = owner
        return self

    def tags(self, *tags: str) -> DAGBuilder:
        """Add tags to the DAG."""
        self._tags.extend(tags)
        return self

    def param(self, key: str, value: str) -> DAGBuilder:
        """Add a parameter with default value."""
        self._params[key] = value
        return self

    def default_retry(self, policy: RetryPolicy) -> DAGBuilder:
        """Set the default retry policy for all tasks."""
        self._default_retry = policy
        return self

    def add_task(self, task_def: TaskDefinition) -> DAGBuilder:
        """Add a task to the DAG."""
        self._tasks.append(task_def)
        return self

    def to_dict(self) -> dict:
        """Convert the DAG to a dictionary."""
        d: Dict[str, Any] = {
            "id": self._id,
            "name": self._name,
        }
        if self._description:
            d["description"] = self._description
        if self._schedule:
            d["schedule"] = self._schedule.to_dict()

        config: Dict[str, Any] = {}
        if self._max_concurrency != 16:
            config["max_concurrency"] = self._max_concurrency
        if self._timeout:
            config["timeout"] = self._timeout
        if self._owner:
            config["owner"] = self._owner
        if self._tags:
            config["tags"] = self._tags
        if self._params:
            config["params"] = self._params
        if self._default_retry:
            config["default_retry"] = self._default_retry.to_dict()
        if config:
            d["config"] = config

        d["tasks"] = [t.to_dict() for t in self._tasks]
        return d

    def to_yaml(self) -> str:
        """Serialize the DAG to YAML."""
        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)

    def to_json(self) -> str:
        """Serialize the DAG to JSON."""
        return json.dumps(self.to_dict(), indent=2)

    def save(self, path: str) -> None:
        """Save the DAG to a YAML file."""
        with open(path, "w") as f:
            f.write(self.to_yaml())


# Decorator-based DAG definition.

_current_dag: Optional[DAGBuilder] = None
_pending_tasks: List[TaskDefinition] = []


def dag(dag_id: str, name: str, **kwargs) -> Callable:
    """Decorator to define a DAG.

    Example:
        @dag("my-dag", "My DAG", schedule="0 * * * *")
        def my_workflow():
            extract_task = extract()
            transform(depends_on=[extract_task])

        dag_def = my_workflow()
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper() -> DAGBuilder:
            global _current_dag, _pending_tasks
            _current_dag = DAGBuilder(dag_id, name, kwargs.get("description", ""))

            if "schedule" in kwargs:
                _current_dag.schedule(kwargs["schedule"])
            if "owner" in kwargs:
                _current_dag.owner(kwargs["owner"])
            if "tags" in kwargs:
                _current_dag.tags(*kwargs["tags"])
            if "timeout" in kwargs:
                _current_dag.timeout(kwargs["timeout"])
            if "max_concurrency" in kwargs:
                _current_dag.max_concurrency(kwargs["max_concurrency"])

            _pending_tasks = []
            func()

            for t in _pending_tasks:
                _current_dag.add_task(t)

            result = _current_dag
            _current_dag = None
            _pending_tasks = []
            return result

        return wrapper
    return decorator


def task(
    task_id: Optional[str] = None,
    executor: str = "python",
    timeout: Optional[str] = None,
    retry: Optional[RetryPolicy] = None,
    priority: int = 0,
    **config_kwargs,
) -> Callable:
    """Decorator to define a task within a DAG.

    Example:
        @task(executor="shell", timeout="5m")
        def extract():
            return {"command": "extract.sh"}
    """
    def decorator(func: Callable) -> Callable:
        tid = task_id or func.__name__

        @functools.wraps(func)
        def wrapper(depends_on: Optional[List[str]] = None) -> str:
            config = func()
            if config is None:
                config = {}
            if config_kwargs:
                config.update(config_kwargs)

            # For Python executor, if no code/script is given, use the function source.
            if executor == "python" and "code" not in config and "script" not in config:
                try:
                    source = inspect.getsource(func)
                    config["code"] = source
                except (OSError, TypeError):
                    pass

            task_def = TaskDefinition(
                id=tid,
                name=func.__name__.replace("_", " ").title(),
                executor=executor,
                config=config,
                dependencies=depends_on or [],
                timeout=timeout,
                retry_policy=retry,
                priority=priority,
            )

            _pending_tasks.append(task_def)
            return tid

        return wrapper
    return decorator
