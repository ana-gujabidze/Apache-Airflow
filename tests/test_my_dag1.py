import unittest

from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State


class TestDAG1(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.dagbag = DagBag()
        self.dag = self.dagbag.get_dag(dag_id='my_dag1')

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
        self.assertListEqual(task_ids, ['slack', ])
        self.assertEqual(len(tasks), 1)

    def test_dependencies_of_slack(self):
        """
        Test checks dependencies of dag with id slack.
        """
        task_id = 'slack'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(upstream_task_ids, [])
        downstream_task_ids = list(map(lambda task: task.task_id, task.downstream_list))
        self.assertListEqual(downstream_task_ids, [])

    def test_execute_no_trigger(self):
        '''
        Test checks state of custome operator after running a task.
        '''
        task = self.dag.get_task('slack')
        ti = TaskInstance(task=task, execution_date=self.dag.default_args.get('start_date'), )
        ti.run(ignore_ti_state=False)
        assert ti.state == State.SUCCESS


suite = unittest.TestLoader().loadTestsFromTestCase(TestDAG1)
unittest.TextTestRunner(verbosity=2).run(suite)
