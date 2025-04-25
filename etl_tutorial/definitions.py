import os
import json
import dagster as dg
from dagster_duckdb import DuckDBResource
from mako.testing.helpers import result_lines


@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion"
)
def products(duckdb : DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE products as (
                SELECT * from  read_csv_auto('data/products.csv')
                )
            """
        )
        preview_query = "SELECT * FROM products LIMIT 10"
        preview_df = conn.execute(preview_query).fetch_df()
        row_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata = {
                "row_count" : dg.MetadataValue.int(count),
                "preview" : dg.MetadataValue.md(preview_df.to_markdown(index = False))
            }
        )

@dg.asset(
    compute_kind = "duckdb",
    group_name = "ingestion"
)
def sales_rep(duckdb : DuckDBResource) ->dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE sales_reps as 
            SELECT * from read_csv_auto('data/sales_reps.csv')
            """
        )
        preview_query = "SELECT * FROM sales_reps LIMIT 10"
        preview_df = conn.execute(preview_query).fetch_df()

        row_count_query = "SELECT COUNT(*) FROM sales_reps"
        row_count = conn.execute(row_count_query).fetchone()

        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count" : dg.MetadataValue.int(count),
                "preview" : dg.MetadataValue.md(preview_df.to_markdown(index = False))
            }
        )


@dg.asset(
    compute_kind = "duckdb",
    group_name = "ingestion"
)
def sales_data(duckdb : DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            DROP TABLE IF EXISTS sales_data;
            CREATE TABLE sales_data AS SELECT * FROM read_csv_auto('data/sales_data.csv')
            """
        )

        preview_query = "SELECT * FROM sales_data LIMIT 10"
        preview_df = conn.execute(preview_query).fetch_df()
        row_count = conn.execute("SELECT COUNT(*) FROM sales_data").fetchone()

        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata = {
                "row_count" : dg.MetadataValue.int(count),
                "preview" : dg.MetadataValue.md(preview_df.to_markdown(index = False))
            }
        )

@dg.asset(
    compute_kind = "duckdb",
    group_name= "joins",
    deps = [sales_data, sales_rep, products]
)
def joined_data(duckdb : DuckDBResource)->dg.MaterializeResult :
    with duckdb.get_connection() as conn:
        conn.execute(
            """
            create or replace view joined_data as (
                select 
                    date,
                    dollar_amount,
                    customer_name,
                    quantity,
                    rep_name,
                    department,
                    hire_date,
                    product_name,
                    category,
                    price
                from sales_data
                left join sales_reps
                    on sales_reps.rep_id = sales_data.rep_id
                left join products
                    on products.product_id = sales_data.product_id
            )
            """
        )
        preview_query = "SELECT * FROM joined_data LIMIT 10"
        preview_df = conn.execute(preview_query).fetch_df()
        count_result_query = "SELECT COUNT(*) FROM joined_data"

        count_result = conn.execute(count_result_query).fetchone()
        result = count_result[0] if count_result else 0

        return dg.MaterializeResult(
            metadata={
                "count" : dg.MetadataValue.int(result),
                "metadata" : dg.MetadataValue.md(preview_df.to_markdown(index = False))
            }
        )

defs = dg.Definitions(
    assets=[products,
            sales_rep,
            sales_data,
            joined_data],
    resources={"duckdb": DuckDBResource(database="data/mydb.duckdb")},
)