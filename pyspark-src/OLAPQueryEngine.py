from pyspark.sql import SparkSession
import time


class OLAPQueryEngine:
    def __init__(self, app_name="OLAP_Query_Engine", parquet_paths=None):
        self._app_name = app_name
        self._parquet_paths = parquet_paths if parquet_paths else {}
        self._spark_engine = (SparkSession.builder
                              .appName(self._app_name)
                              .getOrCreate())
        self._load_data()

    def _load_data(self):
        for table_name, path in self._parquet_paths.items():
            df = self._spark_engine.read.parquet(path)
            df.createOrReplaceTempView(table_name)
            print(f"Table '{table_name}' loaded and registered as a temporary view.")

    def _execute_query(self, query: str):
        result = self._spark_engine.sql(query)
        return result

    def execute_and_show_query_result(self, query: str, num_rows: int = 10, show_all_rows: bool = False, export_path: str = None):
        self._spark_engine.catalog.clearCache()
        start_time = time.time()
        result = self._execute_query(query)
        result_rows_count = result.count()      # action -> triggers actual execution of the transformation specified (query).
        end_time = time.time()

        result.show(result_rows_count if show_all_rows else num_rows)

        query_execution_time = end_time - start_time
        print(f"Query took {query_execution_time:.4f} seconds.")

        if export_path:
            (result
             .coalesce(1)
             .write
             .mode("overwrite")
             .option("header", "false")
             .csv(export_path))

        return result, query_execution_time

    def terminate_spark(self):
        self._spark_engine.stop()
        print("Spark session stopped.")

