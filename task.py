import logging
from typing import List

logger = logging.getLogger(__name__)


class Task:
    def __init__(self, name: str, execution_time: int, group: str, dependencies: List[str]):
        """
        A class representing a single task in a data pipeline.

        Args:
            name (str): The name of the task.
            execution_time (int): The amount of time the task takes to execute.
            group (str): The name of the group this task belongs to.
            dependencies (List[str]): A list of task names that this task depends on.
        """
        self.name = name
        self.execution_time = execution_time
        self.group = group
        self.dependencies = dependencies
        self.num_unresolved_deps = len(self.dependencies)

        # these will be filled in when the task is scheduled
        self.children = []      # tasks that depend on this task
        self.parents = []       # tasks that this task depends on
        self.start_time = 0     # the time when this task starts executing
        self.end_time = 0       # the time when this task finishes executing

        logger.debug(
            f"Created task {name} with execution time {execution_time}, group {group}, and dependencies {dependencies}")

    def add_child(self, task):
        """
        Add a child task to this task's list of dependent tasks.

        Args:
            task (Task): The child task to add.
        """
        self.children.append(task)
        task.parents.append(self)

        logger.debug(f"Task {self.name} added child task {task.name}")

    def done(self, time: int) -> bool:
        """
        Check if the task is done executing.

        Args:
            time (int): The current time.

        Returns:
            bool: True if the task is done, False otherwise.
        """
        return time >= self.end_time

    def __str__(self) -> str:
        """
        Return a string representation of the task.

        Returns:
            str: The string representation of the task.
        """
        return (f"Task(name={self.name}, "
                f"execution_time={self.execution_time}, "
                f"group={self.group}, "
                f"dependencies={self.dependencies}, "
                f"start_time={self.start_time}, "
                f"end_time={self.end_time}, "
                f"children=[{', '.join(child.name for child in self.children)}], "
                f"parents=[{', '.join(parent.name for parent in self.parents)}])")
