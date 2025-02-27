from OLAPQueryEngine import OLAPQueryEngine

spark_client = OLAPQueryEngine(
    parquet_paths={
        # "FCT_LINE_ITEM": "PATH.parquet",
        # "DIM_DETAILED_ORDER": "PATH.parquet",
        # "DIM_DETAILED_PART": "PATH.parquet",
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

spark_client.execute_and_show_query_result(query)
spark_client.terminate_spark()
