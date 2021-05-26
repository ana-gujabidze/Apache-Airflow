import unittest

from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State


class TestDAG2(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.dagbag = DagBag()
        self.dag = self.dagbag.get_dag(dag_id='my_dag2')

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
        self.assertListEqual(task_ids, ['slack_1', 'take_a_break', 'slack_2', ])
        self.assertEqual(len(tasks), 3)

    def test_dependencies_of_slack_1(self):
        """
        Test checks dependencies of dag with id slack_1.
        """
        task_id = 'slack_1'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(upstream_task_ids, [])
        downstream_task_ids = [task.task_id for task in task.downstream_list]
        self.assertListEqual(downstream_task_ids, ['take_a_break', ])

    def test_dependencies_of_take_a_break(self):
        """
        Test checks dependencies of dag with id take_a_break.
        """
        task_id = 'take_a_break'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(upstream_task_ids, ['slack_1', ])
        downstream_task_ids = [task.task_id for task in task.downstream_list]
        self.assertListEqual(downstream_task_ids, ['slack_2', ])

    def test_dependencies_of_print_world(self):
        """
        Test checks dependencies of dag with id slack_2.
        """
        task_id = 'slack_2'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(upstream_task_ids, ['take_a_break', ])
        downstream_task_ids = [task.task_id for task in task.downstream_list]
        self.assertListEqual(downstream_task_ids, [])

    def test_execute_no_trigger(self):
        '''
        Test checks state of custome operator after running a task.
        '''
        task = self.dag.get_task('slack_1')
        ti = TaskInstance(task=task, execution_date=self.dag.default_args.get('start_date'), )
        ti.run(ignore_ti_state=False)
        self.assertEqual(ti.state, State.SUCCESS)


suite = unittest.TestLoader().loadTestsFromTestCase(TestDAG2)
unittest.TextTestRunner(verbosity=2).run(suite)
