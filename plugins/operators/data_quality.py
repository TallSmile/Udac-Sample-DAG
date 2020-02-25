from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_predicates={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_predicates= test_predicates

    def execute(self, context):
        self.log.info('Starting data quality checks')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for key, value in self.test_predicates.items():
            self.log.info("Running {} test".format(key))
            results = redshift.get_records(value["predicate"])
            for result in results:
                if result[0] != value["result"]:
                    raise ValueError("""
                        Result of {} test with predicate
                        {}
                        is not {}
                    """.format(key, value["predicate"], value["result"]))
                self.log.info("{} test PASSED".format(key))
                    