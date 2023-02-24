# Data Pipeline Optimization

This is a Python project that optimizes a data pipeline consisting of multiple tasks with dependencies. The script reads a pipeline definition file, schedules the tasks, and computes the minimum execution time of the pipeline given a specific number of CPU cores to use for execution.

## Problem Statement

You need to optimize a machine learning data pipeline that collects raw data, builds features out of the raw data, and then uses both the raw data and the features to train ML models, and finally build on top of models and train meta-models.

### The structure of the  `pipeline` file:

Each pipeline file consists of `4*n + 1` lines, describing the tasks in the pipeline: 4 lines for each task and a terminating line with value `END`. The properties of each task  are described in 4 consecutive lines:

| Line    | Description                                                   | Example Value
| ------- | ------------| ---------------
|  1      | task name                                                     | A
|  2      | execution time                                                | 3
|  3      | group name, empty line if no group                            | feature
|  4      | comma-separated list of task names that this task depends on, empty line if no group  | B,C,D


## Solution

The solution consists of a Python script `app.py`, which accepts three command-line arguments: `--pipeline`, `--cpu_cores`, and `--log_level`. The script parses the pipeline definition file and computes the minimum execution time of the pipeline, given the number of CPU cores to use for execution. The solution also includes two helper modules, `pipeline.py` and `scheduler.py`, which define the `Pipeline` and `Task` classes and the scheduling algorithm, respectively.

The logger has been added to the project to make it easier to diagnose issues. The logging level can be specified as an optional argument `--log_level` with default value `WARNING` during the execution of the script.

## How to Use

To use the script, you need to have Python 3 installed on your system. You can run the script with the following command:
```
python3 app.py --cpu_cores=N --pipeline=path/to/pipeline.txt --log_level=INFO
```




Replace `path/to/pipeline.txt` with the path to your pipeline definition file, `N` with the number of CPU cores to use for execution, and `INFO` with the desired logging level (DEBUG, INFO, WARNING, ERROR). 

## Scheduling Diagram

The script outputs the minimum execution time of the pipeline and a scheduling diagram that shows which tasks from which group were running at a particular time. The scheduling diagram is a table with the following columns:

- Time: the time when the tasks are executed
- Tasks being executed: the names of the tasks that are being executed at that time
- Group name: the name of the group to which the tasks belong (empty if the task is not part of a group)

## Examples

You can test the script with the provided `pipeline_small.txt` and `pipeline_big.txt` files in the `data` directory. Here's an example of how to run the script and what the output looks like:

```
python3 app.py --cpu_cores=2 --pipeline=data/pipeline.txt --log_level=DEBUG
```


## Tests

The solution also includes a suite of tests in the `tests`
To run the tests, you can use the following command in your terminal:
```
python3 -m unittest discover -s . -p '*_test.py'
```

This command uses the `unittest` module built into Python to discover and run all test files in the `tests` directory that match the pattern `*_test.py`. 
The command should be run in the same directory where your project files and `tests` directory are located.
