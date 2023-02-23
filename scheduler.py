# scheduler_test.py
from collections import deque
from pipeline import Pipeline


def schedule_pipeline(pipeline: Pipeline, cpu_cores: int) -> (int, dict):
    """
    Schedule tasks in a pipeline using priority scheduling.

    :param pipeline: an instance of the Pipeline class
    :param cpu_cores: the number of CPU cores available for executing tasks
    :return: a tuple containing the total execution time and a dictionary representing the scheduling diagram
    """
    # Get all the tasks in the pipeline
    tasks = pipeline.get_tasks()

    # For each task, add it as a child of its dependencies
    for task in tasks:
        for dep_name in task.dependencies:
            dep_task = next(filter(lambda t: t.name == dep_name, tasks))
            dep_task.add_child(task)

    # Sort tasks by group
    groups = {}
    for task in tasks:
        group = task.group or ''
        if group not in groups:
            groups[group] = []
        groups[group].append(task)

    sorted_groups = sorted(groups.keys())
    sorted_tasks = []
    for group in sorted_groups:
        sorted_tasks.extend(sorted(groups[group], key=lambda t: t.name))

    # Schedule tasks using priority scheduling
    schedule = {}
    available_tasks = deque(sorted_tasks)
    in_progress_tasks = []
    end_times = {}
    time = 1  # Start at 1 so that we can handle tasks with execution time of 0 correctly
    current_group = None

    # Loop until all tasks are completed
    while available_tasks or in_progress_tasks:
        # Update the status of in-progress tasks that have finished
        for task in list(in_progress_tasks):
            if end_times[task.name] == time:
                in_progress_tasks.remove(task)
                task.end_time = time

        # Add new tasks to in-progress queue if there is capacity
        while available_tasks and len(in_progress_tasks) < cpu_cores:
            task = available_tasks.popleft()
            if all(dep.end_time is not None for dep in task.parents):
                if current_group is None or task.group == current_group:
                    in_progress_tasks.append(task)
                    task.start_time = time
                    end_times[task.name] = time + task.execution_time
                    current_group = task.group
                else:
                    available_tasks.append(task)
                    break

        # Record the tasks that are being executed at the current time
        for task in in_progress_tasks:
            group = task.group or ''
            if time not in schedule:
                schedule[time] = []
            schedule[time].append((task.name, group))

        # Update the time counter and check if we have completed all tasks in the current group
        if not in_progress_tasks:
            if current_group is None:
                break
            else:
                sorted_groups.remove(current_group)
                if not sorted_groups:
                    break
                current_group = None
        else:
            time += 1

    # The execution time is the time when the last task completed
    execution_time = max(schedule.keys())

    return execution_time, schedule
