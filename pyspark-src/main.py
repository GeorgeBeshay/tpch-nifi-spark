from itertools import accumulate

from OLAPQueryEngine import OLAPQueryEngine

spark_client = OLAPQueryEngine(
    parquet_paths={
        "FCT_LINE_ITEM": "../parquet_files_output/line_item_fact.parquet",
        "DIM_ORDER": "../parquet_files_output/order_dim.parquet",
        "DIM_PART": "../parquet_files_output/part_dim.parquet",
    }
)

query = """
SELECT
    N_NAME,
    S_NAME,
    SUM(L_QUANTITY) AS SUM_QTY,
    SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
    AVG(L_QUANTITY) AS AVG_QTY,
    AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
    AVG(L_DISCOUNT) AS AVG_DISC,
    COUNT(*) AS COUNT_ORDER
FROM
    FCT_LINE_ITEM
INNER JOIN
    DIM_ORDER ON L_ORDERKEY = O_ORDERKEY
INNER JOIN
    DIM_PART ON L_PARTKEY = PS_PARTKEY AND L_SUPPKEY = PS_SUPPKEY
WHERE
    L_SHIPDATE <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
    N_NAME,
    S_NAME;
"""

number_of_runs = 8
query_execution_times = []

for i in range(number_of_runs):
    temp_execution_time = spark_client.execute_and_show_query_result(
        query,
        num_rows=0,
        show_all_rows= (i == number_of_runs-1)
    )
    query_execution_times.append(temp_execution_time)

spark_client.terminate_spark()

for idx, query_execution_time in enumerate(query_execution_times):
    print(f"Run #{idx} - Query took {query_execution_time:.4f} seconds.")
print(f'Average query execution time = {sum(query_execution_times) / len(query_execution_times)}')
