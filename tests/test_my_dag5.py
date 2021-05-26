import datetime
import unittest
from unittest.mock import patch

from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from my_dag5 import choose_best_num


class TestDAG5(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.dagbag = DagBag()
        self.dag = self.dagbag.get_dag(dag_id='my_dag5')

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
        self.assertListEqual(task_ids, ['task_1', 'task_2', 'task_3', 'choose_best', 'accurate', 'inaccurate'])
        self.assertEqual(len(tasks), 6)

    def test_dependencies_of_tasks(self):
        """
        Test checks dependencies of dag with ids task_1, task_2, task_3
        """
        task_ids = ['task_1', 'task_2', 'task_3', ]
        for task_id in task_ids:
            task = self.dag.get_task(task_id)
            upstream_task_ids = [task.task_id for task in task.upstream_list]
            self.assertListEqual(upstream_task_ids, [])
            downstream_task_ids = [task.task_id for task in task.downstream_list]
            self.assertListEqual(downstream_task_ids, ['choose_best'])

    def test_dependencies_of_choose_best(self):
        """
        Test checks dependencies of dag with id choose_best
        """
        task_id = 'choose_best'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(sorted(upstream_task_ids), ['task_1','task_2','task_3'])
        downstream_task_ids = [task.task_id for task in task.downstream_list]
        self.assertListEqual(sorted(downstream_task_ids), ['accurate', 'inaccurate'])

    def test_dependencies_of_accurate_and_inaccurate(self):
        """
        Test checks dependencies of dag with ids accurate, inaccurate
        """
        task_ids = ['accurate', 'inaccurate', ]
        for task_id in task_ids:
            task = self.dag.get_task(task_id)

            upstream_task_ids = [task.task_id for task in task.upstream_list]
            self.assertListEqual(upstream_task_ids, ['choose_best'])
            downstream_task_ids = [task.task_id for task in task.downstream_list]
            self.assertListEqual(downstream_task_ids, [])

    @patch('airflow.models.TaskInstance.xcom_pull', return_value=[2, 5, 9, ])
    def test_choos_best_num1(self, mock_patch):
        '''
        Test checks result of choose_best task in case
        of fixed task_instance.xcom_pull result.
        '''
        result = choose_best_num(TaskInstance)
        self.assertEqual(result, 'accurate')

    @patch('airflow.models.TaskInstance.xcom_pull', return_value=[2, 1, 3, ])
    def test_choos_best_num2(self, mock_patch):
        '''
        Test checks result of choose_best task in case of
        fixed task_instance.xcom_pull result.
        '''
        result = choose_best_num(TaskInstance)
        self.assertEqual(result, 'inaccurate')

    def test_execute_no_trigger(self):
        '''
        Test checks state of custome operator after running a task.
        '''
        task = self.dag.get_task('accurate')
        ti = TaskInstance(task=task, execution_date=datetime.datetime.now(), )
        ti.run(ignore_ti_state=True)
        # If this task is skipped after choose_best task, the test will fail, else it will pass.
        self.assertEqual(ti.state, State.SUCCESS)


suite = unittest.TestLoader().loadTestsFromTestCase(TestDAG5)
unittest.TextTestRunner(verbosity=2).run(suite)
