from google.cloud import bigquery


class BigQueryConnector:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)

    def query(self, query):
        """
        Executes a given SQL query and returns the result.
        """
        query_job = self.client.query(query)
        return query_job.result()

    def list_datasets(self):
        """
        Lists all datasets available in the project.
        """
        datasets = list(self.client.list_datasets())
        return datasets

    def list_tables(self, dataset_id):
        """
        Lists all tables in a given dataset.
        """
        tables = list(self.client.list_tables(dataset_id))
        return tables

    def get_table_schema(self, dataset_id, table_id):
        """
        Retrieves the schema of a given table.
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)
        return table.schema

    # Add more functions as needed, such as loading data, exporting data, etc.
