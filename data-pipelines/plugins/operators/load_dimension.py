from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql,
                 append_data,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_data = append_data

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.append_data:
            redshift_hook.run(self.sql)
        else:
            redshift_hook.run('''DELETE FROM {}'''.format(self.table))
            redshift_hook.run(self.sql)
        self.log.info(f'Loading records into {self.table} completed')
