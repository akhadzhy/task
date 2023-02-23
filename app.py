import argparse
from pipeline import parse_pipeline_file
from scheduler import schedule_pipeline


def main():
    # create argument parser and add arguments
    parser = argparse.ArgumentParser(description='Optimize a data pipeline.')
    parser.add_argument('--pipeline', type=str, required=True, help='path to pipeline file')
    parser.add_argument('--cpu_cores', type=int, required=True, help='number of CPU cores')
    args = parser.parse_args()

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