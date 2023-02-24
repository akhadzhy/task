import argparse
import logging

from pipeline import parse_pipeline_file
from scheduler import schedule_pipeline


def main():
    # create argument parser and add arguments
    parser = argparse.ArgumentParser(description='Optimize a data pipeline.')
    parser.add_argument('--pipeline', type=str, required=True, help='path to pipeline file')
    parser.add_argument('--cpu_cores', type=int, required=True, help='number of CPU cores')
    parser.add_argument('--log_level', type=str, default='WARNING', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='logging level')
    args = parser.parse_args()

    # set up logging
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.log_level)
    logging.basicConfig(level=numeric_level)

    # parse the pipeline file into a list of tasks and their dependencies
    pipeline = parse_pipeline_file(args.pipeline)

    # schedule the tasks in the pipeline and get the execution time and schedule
    execution_time, schedule = schedule_pipeline(pipeline, args.cpu_cores)

    # print the minimum execution time and scheduling diagram
    print(f'Minimum Execution Time = {execution_time} minute')
    print('\nScheduling Diagram:')
    print('| Time | Tasks being Executed | Group Name |')
    print('|------|----------------------|------------|')
    for time, tasks in schedule.items():
        task_names = ', '.join([f"{task[0]}" for task in tasks])
        group_name = tasks[0][1] if tasks else ''
        print(f"|{time: ^6}|{task_names: <22}|{group_name: <12}|")


if __name__ == '__main__':
    main()