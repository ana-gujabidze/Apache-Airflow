from airflow.hooks.base_hook import BaseHook
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.utils.decorators import apply_defaults


class ConnectionSlackPostOperator(SlackAPIPostOperator):
    conn_id = None

    @apply_defaults
    def __init__(self,
                 conn_id=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    def construct_api_call_params(self):
        self.token = BaseHook.get_connection(self.conn_id).password
        super().construct_api_call_params()
