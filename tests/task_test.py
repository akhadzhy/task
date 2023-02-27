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
        task = Task(name='task1', execution_time=10, group='group1', dependencies=['task2', 'task3'])
        task.start_time = 0
        task.end_time = 20
        child1 = Task(name='task2', execution_time=5, group='group1', dependencies=[])
        child2 = Task(name='task3', execution_time=8, group='group2', dependencies=[])
        task.add_child(child1)
        task.add_child(child2)
        expected_output = "Task(name=task1, execution_time=10, group=group1, dependencies=['task2', 'task3'], start_time=0, end_time=20, children=[task2, task3], parents=[])"
        assert str(task) == expected_output
