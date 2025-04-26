import os
import json
import dagster as dg
from dagster import MaterializeResult
from dagster_duckdb import DuckDBResource
from mako.testing.helpers import result_lines


@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion"
)
def products(duckdb: DuckDBResource) -> dg.MaterializeResult:
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
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False))
            }
        )


@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion"
)
def sales_rep(duckdb: DuckDBResource) -> dg.MaterializeResult:
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
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False))
            }
        )


@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion"
)
def sales_data(duckdb: DuckDBResource) -> dg.MaterializeResult:
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
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False))
            }
        )


@dg.asset(
    compute_kind="duckdb",
    group_name="joins",
    deps=[sales_data, sales_rep, products]
)
def joined_data(duckdb: DuckDBResource) -> dg.MaterializeResult:
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
                "count": dg.MetadataValue.int(result),
                "metadata": dg.MetadataValue.md(preview_df.to_markdown(index=False))
            }
        )


@dg.asset_check(asset=joined_data)
def missing_dimension_check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as conn:
        query_result = conn.execute(
            """
            SELECT COUNT(*) FROM joined_data
            where rep_name is NULL or product_name is NULL
            """
        ).fetchone()

        result_count = query_result[0] if query_result else 0
        return dg.AssetCheckResult(
            passed=result_count == 0, metadata={"missing_dimensions": result_count}
        )


monthly_partition = dg.MonthlyPartitionsDefinition(start_date="2024-01-01")


@dg.asset(
    partitions_def=monthly_partition,
    compute_kind="duckdb",
    group_name="analysis",
    deps=[joined_data],
    automation_condition=dg.AutomationCondition.eager()
)
def monthly_sales_performance(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS monthly_sales_performance(
            partition_date VARCHAR,
            rep_name VARCHAR,
            product VARCHAR, 
            total_dollar_amount DOUBLE
            );
            
            DELETE FROM monthly_sales_performance where partition_date = '{month_to_fetch}';
            
            INSERT INTO monthly_sales_performance
            SELECT '{month_to_fetch}' as partition_date,
            rep_name,
            product_name, 
            sum(dollar_amount) as total_dollar_amount
            from joined_data where strftime(date, '%Y-%m') = '{month_to_fetch}'
            group by '{month_to_fetch}', rep_name, product_name;
            """
        )
        preview_query = f"SELECT * FROM monthly_sales_performance where partition_date = '{month_to_fetch}';"
        preview_df = conn.execute(preview_query).fetch_df()
        row_count = conn.execute(
            f"""
            SELECT COUNT(*) 
            from monthly_sales_performance
            where partition_date = '{month_to_fetch}'
            """
        ).fetchone()

        count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False))
        }
    )


product_category_partition = dg.StaticPartitionsDefinition(
    ["Electronics", "Books", "Home and Garden", "Clothing"]
)


@dg.asset(
    deps=[joined_data],
    partitions_def=product_category_partition,
    group_name="analysis",
    compute_kind="duckdb",
    automation_condition=dg.AutomationCondition.eager()
)
def product_performance(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    product_category_str = context.partition_key
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS product_performance(
            product_category VARCHAR,
            product_name VARCHAR,
            total_dollar_amount DOUBLE,
            total_units_sold DOUBLE
            );
            
            DELETE FROM product_performance where product_category = '{product_category_str}';
            
            INSERT INTO product_performance 
            SELECT 
                '{product_category_str}' as product_category,
                product_name,
                SUM(dollar_amount) as total_dollar_amount,
                SUM(quantity) as total_units_sold,
            FROM joined_data
            WHERE category = '{product_category_str}'
            GROUP BY '{product_category_str}', product_name;
            """
        )

        preview_query = f"SELECT * FROM product_performance where product_category = '{product_category_str}';"
        preview_df = conn.execute(preview_query).fetch_df()

        row_count = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM product_performance
            where product_category = '{product_category_str}'
            """
        ).fetchone()

        count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False))
        }
    )


weekly_update_schedule = dg.ScheduleDefinition(
    name="analysis_update_jpb",
    target=dg.AssetSelection.keys("joined_data").upstream(),
    cron_schedule="0 0 * * 1",
)


class AdhocrequestConfig(dg.Config):
    department: str
    product: str
    start_date: str
    end_date: str


@dg.asset(
    deps=[joined_data],
    compute_kind="python"
)
def adhoc_request(config: AdhocrequestConfig, duckdb: DuckDBResource) -> dg.MaterializeResult:
    query = f"""
            SELECT department,
                rep_name,
                product_name,
                sum(dollar_amount) as total_sales,
            FROM joined_data
            WHERE date >= '{config.start_date}'
            and date < '{config.end_date}'
            and department = '{config.department}'
            and product_name = '{config.product}'
            GROUP BY 
                department,
                rep_name, 
                product_name
            """
    with duckdb.get_connection() as conn:
        preview_df = conn.execute(query).fetch_df()
    return dg.MaterializeResult(
        metadata={
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False))
        }
    )


adhoc_request_job = dg.define_asset_job(
    name="adhoc_request_job",
    selection=dg.AssetSelection.assets("adhoc_request"),
)


@dg.sensor(job=adhoc_request_job)
def adhoc_request_sensor(context: dg.SensorEvaluationContext):
    PATH_TO_REQUESTS = os.path.join(os.path.dirname(__file__), "../", "data/requests")

    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    runs_to_request = []

    for filename in os.listdir(PATH_TO_REQUESTS):
        file_path = os.path.join(PATH_TO_REQUESTS, filename)
        if filename.endswith(".json") and os.path.isfile(file_path):
            last_modified = os.path.getmtime(file_path)
            current_state[filename] = last_modified

            if (
                    filename not in previous_state or previous_state[filename] != last_modified
            ):
                with open(file_path) as f:
                    request_config = json.load(f)
                runs_to_request.append(
                    dg.RunRequest(
                        run_key=f"adhoc_request_{filename}_{last_modified}",
                        run_config={
                            "ops": {"adhoc_request": {"config": {**request_config}}}
                        }
                    )
                )
    return dg.SensorResult(
        run_request = runs_to_request, cursor = json.dumps(current_state)
    )
defs = dg.Definitions(
    assets=[products,
            sales_rep,
            sales_data,
            joined_data,
            monthly_sales_performance,
            product_performance,
            adhoc_request],
    asset_checks=[missing_dimension_check],
    schedules=[weekly_update_schedule],
    jobs = [adhoc_request_job],
    sensors = [adhoc_request_sensor],
    resources={"duckdb": DuckDBResource(database="data/mydb.duckdb")},
)

