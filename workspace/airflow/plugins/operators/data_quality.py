from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list=[],
                 primary_key_columns_list=[],
                 results_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list
        self.primary_key_columns_list = primary_key_columns_list
        self.results_list = results_list

    def execute(self, context):
        import pandas as pd
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Create redshift connection
        src_conn = redshift.get_conn()
        src_conn.autocommit = True
        # For each table in the list, check its respective primary key column is not null
        for index, table in enumerate(self.table_list):
            sql_statement = f"""SELECT * FROM {table} WHERE {self.primary_key_columns_list[index]} IS NULL;"""
            cols_df = pd.io.sql.read_sql(sql_statement, con = src_conn)
            assert len(cols_df[self.primary_key_columns_list[index]]) == self.results_list[index]
            self.log.info(f'DataQualityOperator completed for the {self.primary_key_columns_list[index]} column of the {table} table.')
        self.log.info('DataQualityOperator completed for all tables.')