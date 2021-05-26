import unittest

from airflow.models import DagBag


class TestDAG(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.dagbag = DagBag()
        self.dag = self.dagbag.get_dag(dag_id='my_dag')

    def test_dag_loaded(self):
        '''
        Test checks correctness of DAG.
        '''
        self.assertEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(self.dag)

    def test_count_and_contain_tasks(self):
        '''
        Test checks count of and containing tasks.
        '''
        tasks = self.dag.tasks
        task_ids = [task.task_id for task in tasks]
        self.assertListEqual(task_ids, ['print_hello', 'take_a_break', 'print_world', ])
        self.assertEqual(len(tasks), 3)

    def test_dependencies_of_print_hello(self):
        """
        Test checks dependencies of dag with id print_hello.
        """
        task_id = 'print_hello'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(upstream_task_ids, [])
        downstream_task_ids = list(map(lambda task: task.task_id, task.downstream_list))
        self.assertListEqual(downstream_task_ids, ['take_a_break', ])

    def test_dependencies_of_take_a_break(self):
        """
        Test checks dependencies of dag with id take_a_break.
        """
        task_id = 'take_a_break'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(upstream_task_ids, ['print_hello', ])
        downstream_task_ids = list(map(lambda task: task.task_id, task.downstream_list))
        self.assertListEqual(downstream_task_ids, ['print_world', ])

    def test_dependencies_of_print_world(self):
        """
        Test checks dependencies of dag with id print_world.
        """
        task_id = 'print_world'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(upstream_task_ids, ['take_a_break', ])
        downstream_task_ids = list(map(lambda task: task.task_id, task.downstream_list))
        self.assertListEqual(downstream_task_ids, [])


suite = unittest.TestLoader().loadTestsFromTestCase(TestDAG)
unittest.TextTestRunner(verbosity=2).run(suite)
