import csv

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


class SQLToLocalCsv(BaseOperator):

    template_fields = ['sql']
    template_ext = ('.sql')

    @apply_defaults
    def __init__(
            self,
            name=None,
            postgres_conn_id=None,
            sql=None,
            csv=None,
            *args,
            **kwargs
    ) -> None:
        super(SQLToLocalCsv, self).__init__(*args, **kwargs)
        self.name = name
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.csv = csv

    def execute(self, context):
        hook_postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        with hook_postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(self.sql)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                with open(self.csv, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(columns)
                    writer.writerows(rows)
