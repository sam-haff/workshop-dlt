import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdb

@dlt.resource(name="rides")
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page

pipeline_name = "taxi_pipeline"
dataset_name = "ny_taxi"
table_name = "rides"

pipeline = dlt.pipeline(destination="duckdb", pipeline_name=pipeline_name, dataset_name=dataset_name)
load_info = pipeline.run(ny_taxi, table_name=table_name, loader_file_format="parquet", write_disposition="replace")

db_conn = duckdb.connect(f'{pipeline_name}.duckdb')
db_conn.sql(f'SET search_path = {dataset_name}')
print(f'Number of tables: {db_conn.sql("DESCRIBE").df().shape[0]}')

print(f'Number of records extracted: {pipeline.dataset(dataset_type="default").rides.df().shape[0]}')

with pipeline.sql_client() as db_client:
    res = db_client.execute_sql(
        """
        SELECT AVG(DATEDIFF('minute', trip_pickup_date_time, trip_dropoff_date_time))
        FROM rides;
        """
    )
    print(f'Average trip duration {res[0][0]}')
