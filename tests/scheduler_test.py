import unittest
from pipeline import Pipeline
from task import Task
from scheduler import schedule_pipeline


class TestScheduler(unittest.TestCase):
    def test_schedule_pipeline(self):
        pipeline = Pipeline()
        task1 = Task("Task 1", 1, "group1", [])
        pipeline.add_task(task1)
        execution_time, schedule = schedule_pipeline(pipeline, 1)
        self.assertEqual(execution_time, 1)
        self.assertEqual(schedule, {1: [("Task 1", "group1")]})

    def test_schedule_pipeline_multiple_cores(self):
        pipeline = Pipeline()
        task1 = Task("A", 2, "feature", [])
        task2 = Task("B", 1, "feature", [])
        task3 = Task("C", 2, "model", ['B'])
        pipeline.add_task(task1)
        pipeline.add_task(task2)
        pipeline.add_task(task3)
        execution_time, schedule = schedule_pipeline(pipeline, 2)
        self.assertEqual(execution_time, 4)
        self.assertEqual(
            schedule, {
                1: [("A", "feature"), ("B", "feature")],
                2: [("A", "feature")],
                3: [("C", "model")],
                4: [("C", "model")]
            }
        )