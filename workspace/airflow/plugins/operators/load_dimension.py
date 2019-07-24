from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_iam_role="arn:aws:iam::718240196138:role/myRedshiftRole",
                 output_table='',
                 insert_mode='truncate',
                 sql_query='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_iam_role=aws_iam_role
        self.output_table=output_table
        self.insert_mode=insert_mode
        self.sql_query=sql_query

    def execute(self, context):
        # Create connection to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.insert_mode == 'truncate':
            redshift.run(f"TRUNCATE TABLE {self.output_table}") 
        redshift.run(self.sql_query)
        self.log.info(f'LoadDimensionOperator completed for the {self.output_table} table.')
        
        
        
