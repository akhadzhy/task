# task.py
from typing import List


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

        # these will be filled in when the task is scheduled
        self.children = []      # tasks that depend on this task
        self.parents = []       # tasks that this task depends on
        self.start_time = 0     # the time when this task starts executing
        self.end_time = 0       # the time when this task finishes executing

    def add_child(self, task):
        """
        Add a child task to this task's list of dependent tasks.

        Args:
            task (Task): The child task to add.
        """
        self.children.append(task)
        task.parents.append(self)

    def __str__(self):
        """
        Return a string representation of the task.

        Returns:
            str: The string representation of the task.
        """
        return f'{self.name} ({self.execution_time}) {self.dependencies}'
