from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    dimension_sql = """
    INSERT INTO {}
    VALUES (
            SELECT DISTINCT {}
            FROM {}
            {}
    )
    {}
    """
    where_clause = 'WHERE {}'
    on_conflict_clause = 'ON CONFLICT ({}) DO {}'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 origin_table="",
                 origin_columns="",
                 destination_table="",
                 destination_columns="",
                 where="",
                 conflict_column="",
                 on_conflict_do="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.origin_table = origin_table
        self.origin_columns = origin_columns
        self.destination_table = destination_table
        self.where = where
        self.conflict_column = conflict_column
        self.on_conflict_do = on_conflict_do

    def execute(self, context):
        self.log.info(f'Loading Dimension Table {self.destination_table}')

        # create redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # format the columns to select
        format_columns = ', '.join([x + ' AS ' + y for x, y in
                                    zip(self.origin_columns,
                                        self.destination_columns)])

        # format the where clause if applicable
        format_where = ""
        if(self.where != ""):
            format_where = self.where_clause.format(self.where)

        # format the conflict clause if applicable
        format_conflict = ""
        if(self.conflict_column != ""):
            format_conflict = self.conflict_clause.format(self.conflict_column,
                                                          self.on_conflict_do)

        format_sql = self.dimension_sql.format(self.destination_table,
                                               format_columns,
                                               self.origin_table,
                                               format_where,
                                               format_conflict)

        redshift.run(format_sql)
