import json
import unittest
from unittest.mock import Mock, patch

import requests
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from libs.utils import current_weather


class TestDAG6(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.dagbag = DagBag()
        self.dag = self.dagbag.get_dag(dag_id='my_dag6')

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
        self.assertListEqual(task_ids, ['get_weather', 'send_weather', ])
        self.assertEqual(len(tasks), 2)

    def test_dependencies_of_get_weather(self):
        """
        Test checks dependencies of dag with id get_weather.
        """
        task_id = 'get_weather'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(upstream_task_ids, [])
        downstream_task_ids = [task.task_id for task in task.downstream_list]
        self.assertListEqual(downstream_task_ids, ['send_weather', ])

    def test_dependencies_of_send_weather(self):
        """
        Test checks dependencies of dag with id send_weather.
        """
        task_id = 'send_weather'
        task = self.dag.get_task(task_id)

        upstream_task_ids = [task.task_id for task in task.upstream_list]
        self.assertListEqual(upstream_task_ids, ['get_weather', ])
        downstream_task_ids = [task.task_id for task in task.downstream_list]
        self.assertListEqual(downstream_task_ids, [])

    response = {
                "weather": [
                    {
                        "id": 800,
                        "main": "Clear",
                        "description": "clear sky",
                        "icon": "01d"
                    }
                ],
                "main": {
                    "temp": 9.4,
                    "pressure": 1023,
                    "humidity": 100
                },
                "wind": {
                    "speed": 1.5,
                    "deg": 350
                },
            }

    @patch('requests.get', return_value=Mock(text=json.dumps(response)))
    def test_current_weather(self, mock_patch):
        result = current_weather('********************************')
        self.assertEqual(result, 'Current weather in Tbilisi: clear sky\nTemperature in degrees Celsius: 9.4\nHumidity: 100\nWind speed: 1.5')
        requests.get.assert_called_once_with('http://api.openweathermap.org/data/2.5/weather?q=Tbilisi&APPID=********************************&units=metric')

    def test_execute_no_trigger(self):
        '''
        Test checks state of custome operator after running a task.
        '''
        task = self.dag.get_task('send_weather')
        ti = TaskInstance(task=task, execution_date=self.dag.default_args.get('start_date'), )
        ti.run(ignore_ti_state=True)
        self.assertEqual(ti.state, State.SUCCESS)


suite = unittest.TestLoader().loadTestsFromTestCase(TestDAG6)
unittest.TextTestRunner(verbosity=2).run(suite)
