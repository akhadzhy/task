import unittest
from pipeline import Pipeline
from scheduler import Scheduler
from task import Task


class TestScheduler(unittest.TestCase):
    def test_schedule_pipeline(self):
        # Arrange
        pipeline = Pipeline()
        task1 = Task("Task 1", 1, "group1", [])
        pipeline.add_task(task1)
        cpu_cores = 1
        # Act
        scheduler = Scheduler(pipeline.tasks, cpu_cores)
        execution_time, schedule = scheduler.schedule_pipeline()
        # Assert
        self.assertEqual(execution_time, 1)
        self.assertEqual(schedule[1][0].name, task1.name)
        self.assertEqual(schedule[1][0].group, task1.group)
        self.assertEqual(schedule[1][0].execution_time, task1.execution_time)

    def test1_schedule_pipeline_multiple_cores(self):
        # Arrange
        pipeline = Pipeline()
        task1 = Task("A", 2, "feature", [])
        task2 = Task("B", 1, "feature", [])
        task3 = Task("C", 2, "model", ['B'])
        pipeline.add_tasks([task1, task2, task3])
        cpu_cores = 2
        scheduler = Scheduler(pipeline.tasks, cpu_cores)
        # Act
        execution_time, schedule = scheduler.schedule_pipeline()
        # Assert
        self.assertEqual(execution_time, 4)
        self.assertEqual(schedule[1][0].name, task1.name)
        self.assertEqual(schedule[1][1].name, task2.name)
        self.assertEqual(schedule[2][0].name, task1.name)
        self.assertEqual(schedule[3][0].name, task3.name)
        self.assertEqual(schedule[4][0].name, task3.name)

    def test2_schedule_pipeline_multiple_cores(self):
        # Arrange
        pipeline = Pipeline()
        cpu_cores = 2
        task1 = Task("A", 1, "group1", [])
        task2 = Task("B", 1, "group1", ['A'])
        task3 = Task("C", 1, "group1", ['A'])
        task4 = Task("D", 1, "group1", ['A'])
        task5 = Task("E", 1, "group1", ['C', 'D'])
        # Act
        pipeline.add_tasks([task1, task2, task3, task4, task5])
        scheduler = Scheduler(pipeline.tasks, cpu_cores)
        execution_time, schedule = scheduler.schedule_pipeline()
        # Assert
        self.assertEqual(execution_time, 3)
        self.assertEqual(schedule[1][0].name, task1.name)
        self.assertEqual(schedule[2][0].name, task3.name)
        self.assertEqual(schedule[2][1].name, task4.name)
        self.assertEqual(schedule[3][0].name, task2.name)
        self.assertEqual(schedule[3][1].name, task5.name)
