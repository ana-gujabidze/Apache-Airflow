import unittest

from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from freezegun import freeze_time
from libs.utils import find_time_v_2


class TestDAG4(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.dagbag = DagBag()
        self.dag = self.dagbag.get_dag(dag_id='my_dag4')

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
        self.assertListEqual(task_ids, ['get_time', 'slack', ])
        self.assertEqual(len(tasks), 2)

    def test_dependencies_of_get_tim(self):
        """
        Test checks dependencies of dag with id get_tim.
        """
        task_id = 'get_time'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(upstream_task_ids, [])
        downstream_task_ids = [task.task_id for task in task.downstream_list]
        self.assertListEqual(downstream_task_ids, ['slack', ])

    def test_dependencies_of_slack(self):
        """
        Test checks dependencies of dag with id slack.
        """
        task_id = 'slack'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(upstream_task_ids, ['get_time', ])
        downstream_task_ids = [task.task_id for task in task.downstream_list]
        self.assertListEqual(downstream_task_ids, [])

    def test_find_time(self):
        '''
        Test checks if find_time() function returns
        proper value under constant value.
        '''
        with freeze_time("2021-05-20 15:00"):
            result = find_time_v_2()
            # 19 hour because of time difference in find_time() function.
            self.assertEqual(result, 'It is 19 hours.')

    def test_execute_no_trigger(self):
        '''
        Test checks state of custome operator after running a task.
        '''
        task = self.dag.get_task('slack')
        ti = TaskInstance(task=task, execution_date=self.dag.default_args.get('start_date'), )
        ti.run(ignore_ti_state=False)
        self.assertEqual(ti.state, State.SUCCESS)


suite = unittest.TestLoader().loadTestsFromTestCase(TestDAG4)
unittest.TextTestRunner(verbosity=2).run(suite)
