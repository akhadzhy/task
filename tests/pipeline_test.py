import unittest
from pipeline import Pipeline, parse_pipeline_file, Task


class TestPipeline(unittest.TestCase):
    def test_add_task(self):
        # create a pipeline and add a task to it
        pipeline = Pipeline()
        task = Task('A', 2, 'feature', [])
        pipeline.add_task(task)

        # verify that the task was added to the pipeline
        self.assertEqual(len(pipeline.tasks), 1)
        self.assertEqual(pipeline.tasks[0], task)

    def test_get_tasks(self):
        # create a pipeline with two tasks
        pipeline = Pipeline()
        task1 = Task('A', 2, 'feature', [])
        task2 = Task('B', 1, 'feature', ['A'])
        pipeline.add_task(task1)
        pipeline.add_task(task2)

        # verify that the tasks can be retrieved from the pipeline
        tasks = pipeline.get_tasks()
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0], task1)
        self.assertEqual(tasks[1], task2)

    def test_parse_pipeline_file(self):
        # test parsing a pipeline file with two tasks and their dependencies
        pipeline = parse_pipeline_file('data/pipeline.txt')

        # verify that the pipeline was parsed correctly
        self.assertEqual(len(pipeline.tasks), 3)
        self.assertEqual(pipeline.tasks[0].name, 'A')
        self.assertEqual(pipeline.tasks[1].name, 'B')
        self.assertEqual(pipeline.tasks[2].name, 'C')
        self.assertEqual(pipeline.tasks[2].dependencies, ['B'])
