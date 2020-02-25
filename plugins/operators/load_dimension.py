from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dim_table="",
                 insertion_table="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = insertion_table
        self.dim_table = dim_table
        self.truncate = truncate

    def execute(self, context):
        self.log.info("Starting execution of load operator for {}".format(self.dim_table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = """
            {}
            INSERT INTO {}
            {}
        """.format("TRUNCATE TABLE {};".format(self.dim_table) if self.truncate else "",
                   self.dim_table,
                   self.sql_statement)
        self.log.info('Executing load operation')
        redshift.run(formatted_sql)
