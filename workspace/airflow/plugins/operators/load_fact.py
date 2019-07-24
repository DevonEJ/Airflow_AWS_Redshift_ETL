from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_iam_role="arn:aws:iam::718240196138:role/myRedshiftRole",
                 output_table='',
                 sql_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_iam_role = aws_iam_role
        self.output_table = output_table
        self.sql_query = sql_query

    def execute(self, context):
        # Create connection to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.sql_query)
        self.log.info(f'LoadFactOperator completed for the {self.output_table} table.')
