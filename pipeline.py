# pipline.py
from task import Task
import logging

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self):
        self.tasks = []

    def add_task(self, task: Task):
        """
        Add a task to the pipeline.

        Args:
            task (Task): The task to add.
        """
        self.tasks.append(task)

    def get_tasks(self):
        """
        Get the list of tasks in the pipeline.

        Returns:
            list: The list of tasks in the pipeline.
        """
        return self.tasks


def parse_pipeline_file(file_path: str) -> Pipeline:
    """
    Parse a pipeline file into a `Pipeline` object.

    Args:
        file_path (str): The path to the pipeline file.

    Returns:
        Pipeline: A `Pipeline` object containing the tasks in the file.
    """
    pipeline = Pipeline()
    logger.info(f"Parsing pipeline file: {file_path}")
    with open(file_path, 'r') as f:
        while True:
            task_name = f.readline().strip()
            if not task_name:
                break

            # check if the task is the end of the pipeline
            if task_name == 'END':
                break

            execution_time = int(f.readline().strip())
            group = f.readline().strip() or None

            dependencies = f.readline().strip()
            if not dependencies:
                dependencies = []
            else:
                dependencies = dependencies.split(',')

            # Check for circular dependencies
            if task_name in dependencies:
                raise ValueError(f"Circular dependency detected: {task_name} depends on itself")

            pipeline.add_task(Task(task_name, execution_time, group, dependencies))

    logger.info(f"Parsed {len(pipeline.get_tasks())} pipeline tasks")
    return pipeline
