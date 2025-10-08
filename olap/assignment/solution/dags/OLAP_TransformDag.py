import pendulum
import logging
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)
output_folder = "/opt/airflow/data"

fact_table = "FactSales"

def failure_alert(context):
    exc = context.get("exception")
    ti = context.get("ti")
    logger.error(
        f'FAILED task: dag={getattr(ti, "dag_id")}, task_id={getattr(ti, "task_id")}, run_id={context.get("run_id")}, try_number={getattr(ti, "try_number")}'
    )
    logger.exception(exc)

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

with DAG(
    dag_id="olap_dag",
    start_date=START_DATE,
    schedule="0 0 * * *",
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(seconds=10),
        "on_failure_callback": failure_alert
    },
    template_searchpath=["/opt/airflow/data/"],
    tags=["olap"]
) as dag:

    def _transform_oltp_to_olap(fact_table: str):
        # --- Read from OLTP ---
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        categories = pd.read_sql("SELECT * FROM categories;", conn)
        suppliers = pd.read_sql("SELECT * FROM suppliers;", conn)
        products = pd.read_sql("SELECT * FROM products;", conn)
        stores = pd.read_sql("SELECT * FROM stores;", conn)
        customers = pd.read_sql("SELECT * FROM customers;", conn)
        purchases = pd.read_sql("SELECT * FROM purchases;", conn)
        purchase_items = pd.read_sql("SELECT * FROM purchase_items;", conn)
        payments = pd.read_sql("SELECT * FROM payments;", conn)

        # --- Transformations ---
        dim_date = (
            purchases[["purchasedatetime"]]
            .drop_duplicates()
            .rename(columns={"purchasedatetime": "fulldate"})
        )
        dim_date["datekey"] = range(1, len(dim_date) + 1)
        dim_date["year"] = dim_date["fulldate"].dt.year
        dim_date["month"] = dim_date["fulldate"].dt.month
        dim_date["day"] = dim_date["fulldate"].dt.day
        dim_date["dayofweek"] = dim_date["fulldate"].dt.day_name()

        dim_store = stores.rename(
            columns={"storeid": "storekey", "storename": "storename", "location": "city"}
        )
        dim_store["region"] = None

        dim_category = categories.rename(
            columns={"categoryid": "categorykey", "categoryname": "category"}
        )

        dim_supplier = suppliers.rename(
            columns={"supplierid": "supplierkey", "suppliername": "suppliername"}
        )
        dim_supplier["contactinfo"] = None

        dim_product = (
            products.merge(dim_category, left_on="categoryid", right_on="categorykey", how="left")
            .rename(columns={"productid": "productkey", "productname": "productname", "category": "category"})
            [["productkey", "productname", "category", "price"]]
        )
        dim_product["brand"] = None

        dim_customer = customers.rename(
            columns={"customerid": "customerkey", "firstname": "firstname", "lastname": "lastname"}
        )
        dim_customer["segment"] = 'Regular'
        dim_customer["city"] = None
        dim_customer["validfrom"] = datetime.now().date()
        dim_customer["validto"] = '9999-12-31'

        dim_payment = payments[["paymentid", "paymentmethod"]].rename(
            columns={"paymentid": "paymentkey", "paymentmethod": "paymenttype"}
        )

        fact_sales = (
            purchase_items
            .merge(purchases, on="purchaseid", how="left")
            .merge(products, on="productid", how="left")
        )

        # Merge payments safely
        if "purchaseid" in payments.columns:
            fact_sales = fact_sales.merge(payments, on="purchaseid", how="left")
            if "paymentid" in fact_sales.columns:
                fact_sales = fact_sales.rename(columns={"paymentid": "paymentkey"})
            else:
                fact_sales["paymentkey"] = None  # fallback if no paymentid
        else:
            fact_sales["paymentkey"] = None
        fact_sales = fact_sales.merge(dim_date, left_on="purchasedatetime", right_on="fulldate", how="left")
        
        fact_sales["salesamount"] = fact_sales["quantity"] * fact_sales["price"]

        fact_sales = fact_sales[[
            "purchaseid", "datekey", "storeid", "productid", "customerid", "paymentkey",
            "quantity", "salesamount"
        ]].rename(columns={"purchaseid": "saleid"})

        tables = {
            "dimdate": dim_date,
            "dimstore": dim_store,
            "dimproduct": dim_product,
            "dimsupplier": dim_supplier,
            "dimcustomer": dim_customer,
            "dimpayment": dim_payment,
            fact_table.lower(): fact_sales
        }

        output_sql_path = "/opt/airflow/data/create_database_olap.sql"

        with open(output_sql_path, "w", encoding="utf-8") as f:
            f.write("-- OLAP Database Schema and Data Inserts\n")
            f.write("BEGIN;\n\n")

            for name, df in tables.items():
                f.write(f"-- Table: {name}\n")
                f.write(f"DROP TABLE IF EXISTS {name} CASCADE;\n")

                # --- CREATE TABLE ---
                if name == fact_table.lower():
                    # Fact table with relationships
                    create_stmt = f"""
                    CREATE TABLE {name} (
                        saleid INTEGER,
                        datekey INTEGER,
                        storeid INTEGER,
                        productid INTEGER,
                        customerid INTEGER,
                        paymentkey INTEGER,
                        quantity INTEGER,
                        salesamount DECIMAL(18,2),
                        FOREIGN KEY (datekey) REFERENCES dimdate(datekey),
                        FOREIGN KEY (storeid) REFERENCES dimstore(storekey),
                        FOREIGN KEY (productid) REFERENCES dimproduct(productkey),
                        FOREIGN KEY (customerid) REFERENCES dimcustomer(customerkey),
                        FOREIGN KEY (paymentkey) REFERENCES dimpayment(paymentkey)
                    );
                    """
                    f.write(create_stmt)
                else:
                    # Dimension tables dynamically with PRIMARY KEY
                    create_stmt = "CREATE TABLE " + name + " (\n"
                    for col, dtype in df.dtypes.items():
                        if "int" in str(dtype):
                            sql_type = "INTEGER"
                        elif "float" in str(dtype):
                            sql_type = "DECIMAL(18,2)"
                        elif "datetime" in str(dtype):
                            sql_type = "TIMESTAMP"
                        else:
                            sql_type = "VARCHAR(255)"
                        # Set primary key for known keys
                        pk_cols = {
                            "dimdate": "datekey",
                            "dimstore": "storekey",
                            "dimproduct": "productkey",
                            "dimsupplier": "supplierkey",
                            "dimcustomer": "customerkey",
                            "dimpayment": "paymentkey"
                        }
                        if name in pk_cols and col == pk_cols[name]:
                            create_stmt += f"    {col} {sql_type} PRIMARY KEY,\n"
                        else:
                            create_stmt += f"    {col} {sql_type},\n"
                    create_stmt = create_stmt.rstrip(",\n") + "\n);\n"
                    f.write(create_stmt)

                # --- INSERT INTO ---
                if not df.empty:
                    f.write(f"\n-- Insert data into {name}\n")
                    for _, row in df.iterrows():
                        values = []
                        for v in row:
                            if pd.isna(v):
                                values.append("NULL")
                            elif isinstance(v, (pd.Timestamp, datetime)):
                                values.append(f"'{v}'")
                            elif isinstance(v, str):
                                safe_v = v.replace("'", "''")
                                values.append(f"'{safe_v}'")
                            else:
                                values.append(str(v))
                        insert_stmt = f"INSERT INTO {name} VALUES ({', '.join(values)});\n"
                        f.write(insert_stmt)
                f.write("\n\n")

            f.write("COMMIT;\n")


    oltp_to_olap = PythonOperator(
        task_id="oltp_to_olap",
        python_callable=_transform_oltp_to_olap,
        op_kwargs={"fact_table": fact_table}
    )

    load_olap = SQLExecuteQueryOperator(
        task_id = "load_olap",
        conn_id = 'olap',
        sql="create_database_olap.sql",
        trigger_rule = 'none_failed',
        autocommit = True
    )

    oltp_to_olap >> load_olap
