import logging
from collections import defaultdict
from typing import List, Tuple
from task import Task
from dag import DAG
from copy import deepcopy

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, tasks: List[Task], max_concurrency: int):
        """
        A class to schedule a set of tasks in a data pipeline.

        Args:
            tasks (List[Task]): A list of Task objects.
            max_concurrency (int): The maximum number of threads to use.
        """
        self.dag = DAG(tasks)
        self.max_concurrency = max_concurrency
        assert max_concurrency >= 1, "Invalid maximum number of threads! Must be a positive integer"

    def schedule_pipeline(self) -> Tuple[int, defaultdict[int, list]]:
        """
        Schedule the tasks in the data pipeline.

        Returns:
            Dict[str, List[Task]]: A dictionary of tasks to be executed at each time step.
        """
        graph = deepcopy(self.dag.graph_ids)
        tasks_dict = deepcopy(self.dag.node_dict)
        schedule = defaultdict(list)
        done = set()
        running = set()
        time = 0

        def get_highest_priority_group(tasks: List[Task]) -> str:
            group_priority = defaultdict(int)
            for task in tasks:
                group_priority[task.group] += task.compound_priority
            # Find the group with the highest total priority
            highest_group = max(group_priority, key=group_priority.get)
            return highest_group

        while len(graph):
            # Check if there are completed tasks from the running ones
            if running:
                for task in running:
                    schedule[time] += [task]
                done_ = [task for task in running if task.done(time)]
                done.update(done_)
                running.difference_update(done_)
                for task in done_:
                    logger.debug(f"Remove Task {task.name} from the graph")
                    graph.remove_node(task.name)

            # Get the number of running tasks
            num_running_threads = len(running)
            # List the root tasks that aren't being executed
            runnable_tasks_ids = list(set(graph.root_nodes()) - set(task.name for task in done) - set(task.name for task in running))
            # If no runnable task exists, wait for a task to finish
            if not runnable_tasks_ids:
                logger.debug("No runnable Tasks available")
                time += 1
                continue

            # Choose a task to run
            # Get the most prioritized task to run
            runnable_tasks = [tasks_dict[task_name] for task_name in runnable_tasks_ids]
            running_groups = [task.group for task in running if task.group is not None]
            if running_groups:
                highest_priority_groups = running_groups[0]
            else:
                highest_priority_groups = get_highest_priority_group(runnable_tasks)
            runnable_tasks_per_group_limit = [task for task in runnable_tasks if task.group == highest_priority_groups or task.group is None]

            # Get the tasks with the highest compound priority
            # (randomly selected if multiple are suggested)
            highest_priority_tasks_sorted = sorted(runnable_tasks_per_group_limit, key=lambda task: task.compound_priority)

            # Check how many threads are available
            available_threads = self.max_concurrency - num_running_threads
            # If there are available threads, run a Task
            if available_threads > 0:
                exec_tasks = highest_priority_tasks_sorted[-available_threads:]
                for exec_task in exec_tasks:
                    logger.info(f"{exec_task.name} will run!")
                    exec_task.start_time = time
                    exec_task.end_time = time + exec_task.execution_time
                    running.add(exec_task)

            time += 1

        # Sort the tasks by name
        # it is not necessary, but it makes the output more stable for testing
        for time in schedule:
            schedule[time] = sorted(schedule[time], key=lambda task: task.name)

        # schedule execution time is the last execution time of the last task
        execution_time = max(schedule.keys())
        return execution_time, schedule
