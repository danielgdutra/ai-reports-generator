from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict
from dotenv import load_dotenv
import os, json


PROJECT_ID = 'ai-reports-generator'


def make_schema_field(field):
    schema = None
    allowed_types = ['STRING', 'BYTES', 'INTEGER', 'FLOAT', 'BOOLEAN', 'TIMESTAMP', 'DATE', 'TIME', 'DATETIME',
                     'GEOGRAPHY', 'NUMERIC', 'BIGNUMERIC', 'RECORD', 'INT64', 'FLOAT64', 'BOOL', 'STRUCT', 'JSON']
    mode = field.get('mode', 'NULLABLE')
    name = field.get('name', None)
    field_type = field.get('type', None)
    fields = field.get('fields', None)
    description = field.get('description', '')
    if not (name and name.strip()):
        raise SchemaCreationError("Field name cannot be None or empty")
    elif not (field_type and field_type.strip() and field_type in allowed_types):
        raise SchemaCreationError(
            "Field type must match the following: {}".format(', '.join(allowed_types)))
    else:
        if field_type in ['RECORD', 'STRUCT'] and fields is None:
            raise SchemaCreationError(
                "STRUCT and RECORD field type must contain field specification")
        elif field_type in ['RECORD', 'STRUCT']:
            nested_fields = []
            for f in fields:
                nested_fields.append(make_schema_field(f))
            schema = bigquery.SchemaField(name=name, field_type=field_type, description=description, mode=mode,
                                          fields=nested_fields)
        else:
            schema = bigquery.SchemaField(name=name, field_type=field_type, description=description, mode=mode)
    if schema:
        return schema
    else:
        return False


class SchemaCreationError(Exception):
    pass


class BigQueryConn:
    def __init__(self, dataset_id, table_id, schema_path=None):
        load_dotenv()
        self.project_id = PROJECT_ID
        self.dataset_id = dataset_id
        self.credentials = service_account.Credentials.from_service_account_info(
            json.loads(os.getenv('BIGQUERY_KEY')), scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.field_list = self.load_field_list(schema_path) if schema_path is not None else []
        self.table_id = '{}.{}.{}'.format(self.project_id, self.dataset_id, table_id)
        self.client = self.get_bq_client()
        self.schema = self.make_schema()

    def get_bq_client(self):
        return bigquery.Client(credentials=self.credentials)

    def load_field_list(self, schema_path: str) -> dict:
        with open(schema_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def make_schema(self):
        return [make_schema_field(field) for field in self.field_list]

    def delete_old_data(self, date_column='reference_date', **kwargs):

        query = f"DELETE {self.table_id} WHERE TRUE"

        if kwargs.get('start_date') and kwargs.get('end_date'):
            query += f" AND DATE({date_column}) BETWEEN '{kwargs.get('start_date')}' AND '{kwargs.get('end_date')}'"

        query += ';'
        self.client.query(query)

    def truncate_table(self):
        query = "TRUNCATE TABLE {};".format(self.table_id)
        self.client.query(query)

    def insert_data_date_partitioned(self, data, partitioned_by=None, partition_type='DAY', clustering_fields=None):
        table = bigquery.Table(self.table_id, schema=self.schema)
        if partitioned_by:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=partition_type,
                field=partitioned_by
            )
        if clustering_fields:
            table.clustering_fields = clustering_fields
        job_config = bigquery.LoadJobConfig(schema=self.schema, autodetect=False, schema_update_options="ALLOW_FIELD_ADDITION")
        try:
            self.client.create_table(table)
        except Conflict:
            pass
        finally:
            job = self.client.load_table_from_json(data, self.table_id, job_config=job_config)
            job.result()

    def insert_data(self, data):
        table = bigquery.Table(self.table_id, schema=self.schema)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=self.schema, autodetect=False)
        try:
            self.client.create_table(table)
        except Conflict:
            pass
        finally:
            job = self.client.load_table_from_json(data, self.table_id, job_config=job_config)
            job.result()

    def insert_data_append(self, data, partitioned_by=None, partition_type='DAY', clustering_fields=None):
        table = bigquery.Table(self.table_id, schema=self.schema)
        if partitioned_by:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=partition_type,
                field=partitioned_by
            )
        if clustering_fields:
            table.clustering_fields = clustering_fields
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema_update_options="ALLOW_FIELD_ADDITION", schema=self.schema, autodetect=False)
        try:
            self.client.create_table(table)
        except Conflict:
            pass
        finally:
            job = self.client.load_table_from_json(data, self.table_id, job_config=job_config)
            job.result()

    def query_data(self, query):
        return self.client.query(query=query)
