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

    def execute_and_show_query_result(self, query: str, num_rows: int = 10):
        start_time = time.time()
        result = self._execute_query(query)
        end_time = time.time()
        result.show(num_rows)
        print(f"Query took {(end_time - start_time):.4f} seconds.")

    def terminate_spark(self):
        self._spark_engine.stop()
        print("Spark session stopped.")

