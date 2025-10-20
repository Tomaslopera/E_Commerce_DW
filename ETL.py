import uuid
import io
import boto3
import pandas as pd
import sqlalchemy
import warnings
from prefect import task, flow
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()

warnings.filterwarnings("ignore")

# Conexión a PostgreSQL
engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASS')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}")

# Configuración de AWS S3
s3 = boto3.client("s3")

# =====================================
# EXTRACT
# =====================================
@task(name="Extract from PostgreSQL")
def extract_data(table_name: str) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    print(f"{len(df)} filas extraídas de {table_name}")
    return df


# =====================================
# TRANSFORM
# =====================================
@task(name="Transform data to star schema")
def transform_data(df_customer, df_geolocation, df_order_items, df_order_payments, df_order_reviews, df_orders, df_products, df_sellers):

    # --- Dimensión Customer ---
    dim_customer = df_customer[["customer_id", "customer_unique_id", "customer_zip_code_prefix"]]
    dim_customer = dim_customer.merge(df_orders[["customer_id", "order_id"]], on="customer_id", how="left")
    dim_customer = dim_customer.rename(columns={"customer_id": "customer_key"})

    # --- Dimensión Payment ---
    dim_payment = df_order_payments[["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]]
    dim_payment["payment_key"] = [uuid.uuid4() for _ in range(len(dim_payment))]
    dim_payment = dim_payment[["payment_key", "payment_sequential", "payment_type", "payment_installments", "payment_value", "order_id"]]

    # --- Dimensión Review ---
    dim_review = df_order_reviews.rename(columns={
        "review_id": "review_key",
        "review_answer_timestamp": "review_answer_date"
    })
    dim_review["review_creation_date"] = pd.to_datetime(dim_review["review_creation_date"])
    dim_review["review_answer_date"] = pd.to_datetime(dim_review["review_answer_date"])

    # --- Dimensión Product ---
    dim_product = df_products.rename(columns={
        "product_id": "product_key",
        "product_weight_g": "product_weight",
        "product_length_cm": "product_length",
        "product_height_cm": "product_height",
        "product_width_cm": "product_width"
    })
    dim_product = dim_product.merge(df_order_items[["product_id", "order_id"]],
                                    left_on="product_key", right_on="product_id", how="left")
    dim_product.drop(columns=["product_id"], inplace=True)

    # --- Dimensión Seller ---
    dim_seller = df_sellers.rename(columns={"seller_id": "seller_key"})
    dim_seller = dim_seller.merge(df_order_items[["seller_id", "order_id"]],
                                  left_on="seller_key", right_on="seller_id", how="left")
    dim_seller.drop(columns=["seller_id"], inplace=True)

    # --- Dimensión Geography ---
    dim_geography = df_geolocation.rename(columns={
        "geolocation_zip_code_prefix": "geography_key",
        "geolocation_lat": "geography_lat",
        "geolocation_lng": "geography_lng",
        "geolocation_city": "geography_city",
        "geolocation_state": "geography_state"
    })

    # --- Dimensión Time ---
    dim_time = df_orders[[
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]]
    dim_time["time_key"] = pd.to_datetime(dim_time["order_purchase_timestamp"])
    dim_time["time_shipping_limit"] = pd.to_datetime(df_order_items["shipping_limit_date"])
    dim_time = dim_time.rename(columns={
        "order_approved_at": "time_approved",
        "order_delivered_carrier_date": "time_delivered_carrier",
        "order_delivered_customer_date": "time_delivered_customer",
        "order_estimated_delivery_date": "time_estimated_delivery"
    })

    # --- Fact Table ---
    fact_orders = df_orders[["order_id", "order_status", "order_purchase_timestamp"]]
    fact_orders["order_purchase_timestamp"] = pd.to_datetime(fact_orders["order_purchase_timestamp"])
    fact_orders = fact_orders.merge(df_order_items[["order_id", "order_item_id", "price", "freight_value"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_review[["order_id", "review_key"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_customer[["order_id", "customer_key", "customer_zip_code_prefix"]],
                                    on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_payment[["payment_key", "order_id"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_product[["product_key", "order_id"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_seller[["seller_key", "order_id", "seller_zip_code_prefix"]],
                                    on="order_id", how="left")
    fact_orders = fact_orders.rename(columns={
        "customer_zip_code_prefix": "geography_customer_key",
        "seller_zip_code_prefix": "geography_seller_key",
        "order_status": "status"
    })
    fact_orders.drop_duplicates(inplace=True)

    print("Transformación completa.")
    return dim_customer, dim_payment, dim_review, dim_product, dim_seller, dim_geography, dim_time, fact_orders


# =====================================
# LOAD
# =====================================
@task(name="Load to S3")
def load_to_s3(df, table_name, key_column):
    object_key = f"staging/{table_name}.parquet"

    try:
        response = s3.get_object(Bucket=os.getenv("S3_BUCKET"), Key=object_key)
        existing_df = pd.read_parquet(io.BytesIO(response["Body"].read()))
    except s3.exceptions.NoSuchKey:
        existing_df = pd.DataFrame()

    if not existing_df.empty:
        merged = pd.concat([existing_df, df])
        merged = merged.drop_duplicates(subset=[key_column])
    else:
        merged = df
        
    if 'payment_key' in df.columns:
        df['payment_key'] = df['payment_key'].astype(str)

    buffer = io.BytesIO()
    merged.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=os.getenv("S3_BUCKET"), Key=object_key, Body=buffer.getvalue())

    print(f"{table_name} actualizado → {len(merged)} registros totales en S3.")


# =====================================
# FLOW (Prefect)
# =====================================
@flow(name="ETL Olist to S3 Staging Zone")
def etl_staging_flow():
    # Extraer
    df_customer = extract_data("customers")
    df_geolocation = extract_data("geolocation")
    df_order_items = extract_data("order_items")
    df_order_payments = extract_data("order_payments")
    df_order_reviews = extract_data("order_reviews")
    df_orders = extract_data("orders")
    df_products = extract_data("products")
    df_sellers = extract_data("sellers")

    # Transformar
    dims = transform_data(df_customer, df_geolocation, df_order_items, df_order_payments,
                          df_order_reviews, df_orders, df_products, df_sellers)

    dim_customer, dim_payment, dim_review, dim_product, dim_seller, dim_geo, dim_time, fact_orders = dims

    # Cargar
    load_to_s3(dim_customer, "dim_customer", "customer_key")
    load_to_s3(dim_payment, "dim_payment", "payment_key")
    load_to_s3(dim_review, "dim_review", "review_key")
    load_to_s3(dim_product, "dim_product", "product_key")
    load_to_s3(dim_seller, "dim_seller", "seller_key")
    load_to_s3(dim_geo, "dim_geography", "geography_key")
    load_to_s3(dim_time, "dim_time", "time_key")
    load_to_s3(fact_orders, "fact_orders", "order_id")

    print("ETL completo y datos cargados a zona de staging (S3).")


if __name__ == "__main__":
    etl_staging_flow()