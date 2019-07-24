from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    staging_sql_statement = """ COPY {table} FROM '{s3}'
    iam_role '{arn}' 
    region '{region}' 
    FORMAT AS JSON '{json_path}';"""
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_iam_role="arn:aws:iam::718240196138:role/myRedshiftRole",
                 aws_s3_bucket="",
                 destination_staging_table="",
                 aws_s3_region="us-west-2",
                 format_as_json_path="auto",
                 table_create_statement="",
                 sql_statement=staging_sql_statement,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_iam_role = aws_iam_role
        self.aws_s3_bucket = aws_s3_bucket
        self.destination_staging_table = destination_staging_table
        self.aws_s3_region = aws_s3_region
        self.format_as_json_path = format_as_json_path
        self.sql_statement = sql_statement

    def execute(self, context):
        # Create connection to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Format the SQL statement, given the arguments passed 
        formatted_sql = self.sql_statement.format(
            table = self.destination_staging_table,
            s3 = self.aws_s3_bucket,
            arn = self.aws_iam_role,
            region = self.aws_s3_region,
            json_path = self.format_as_json_path  
        )
        # Execute SQL
        redshift.run(formatted_sql)
        self.log.info(f'StageToRedshiftOperator completed for the {self.destination_staging_table} table.')


