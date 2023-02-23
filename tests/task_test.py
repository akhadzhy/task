import unittest
from task import Task


class TaskTestCase(unittest.TestCase):

    def test_task_attributes(self):
        task = Task('test', 5, 'group1', ['dep1', 'dep2'])
        self.assertEqual(task.name, 'test')
        self.assertEqual(task.execution_time, 5)
        self.assertEqual(task.group, 'group1')
        self.assertEqual(task.dependencies, ['dep1', 'dep2'])

    def test_add_child(self):
        task1 = Task('task1', 3, 'group1', [])
        task2 = Task('task2', 4, 'group1', [])
        task1.add_child(task2)
        self.assertIn(task2, task1.children)
        self.assertIn(task1, task2.parents)

    def test_str(self):
        task = Task('test', 5, 'group1', ['dep1', 'dep2'])
        self.assertEqual(str(task), "test (5) ['dep1', 'dep2']")
