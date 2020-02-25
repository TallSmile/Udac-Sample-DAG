from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 schema="public",
                 insertion_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = insertion_table
        self.schema = schema

    def execute(self, context):
        self.log.info('Starting load operation execution')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = """
            INSERT INTO {}.songplays {}
        """.format(self.schema, self.sql_statement)
        self.log.info('Running SQL query')
        redshift.run(formatted_sql)
