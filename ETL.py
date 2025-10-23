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
@task(name="Extract from PostgreSQL with sampling")
def extract_data(table_name: str) -> pd.DataFrame:
    # Definir proporciones personalizadas por tabla
    sample_fractions = {
        "customers": 1,
        "geolocation": 1,   # 0.5%
        "order_items": 1,
        "order_payments": 1,
        "order_reviews": 1,
        "orders": 1,
        "products": 1,
        "sellers": 1
    }

    frac = sample_fractions.get(table_name, 1.0)

    # Ejecutar la consulta
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)

    # Aplicar el muestreo solo si el tamaño > 0
    if len(df) > 0 and frac < 1.0:
        df = df.sample(frac=frac, random_state=42).reset_index(drop=True)

    print(f"{len(df)} filas extraídas de {table_name} ({frac*100:.1f}%)")
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
    dim_review = df_order_reviews[["review_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp", "order_id"]]
    dim_review = dim_review.rename(columns={"review_id": "review_key", "review_answer_timestamp": "review_answer_date"})
    dim_review["review_creation_date"] = pd.to_datetime(dim_review["review_creation_date"])
    dim_review["review_answer_date"] = pd.to_datetime(dim_review["review_answer_date"])

    # --- Dimensión Product ---
    dim_product = df_products[["product_id", "product_category_name", "product_name_length", "product_description_length", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]]
    dim_product = dim_product.merge(df_order_items[["product_id", "order_id"]], on="product_id", how="left")
    dim_product = dim_product.rename(columns={"product_id": "product_key", "product_weight_g": "product_weight", "product_length_cm": "product_length", "product_height_cm": "product_height", "product_width_cm": "product_width"})

    # --- Dimensión Seller ---
    dim_seller = df_sellers[["seller_id", "seller_zip_code_prefix"]]
    dim_seller = dim_seller.merge(df_order_items[["seller_id", "order_id"]], on="seller_id", how="left")
    dim_seller = dim_seller.rename(columns={"seller_id": "seller_key"})

    # --- Dimensión Geography ---
    dim_geography = df_geolocation[["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"]]
    dim_geography = dim_geography.rename(columns={"geolocation_zip_code_prefix": "geography_key", "geolocation_lat": "geography_lat", "geolocation_lng": "geography_lng", "geolocation_city": "geography_city", "geolocation_state": "geography_state"})

    # --- Dimensión Time ---
    dim_time = df_orders[["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]]
    dim_time["time_shipping_limit"] = df_order_items["shipping_limit_date"]
    dim_time = dim_time.rename(columns={"order_purchase_timestamp": "time_key", "order_approved_at": "time_approved", "order_delivered_carrier_date": "time_delivered_carrier", "order_delivered_customer_date": "time_delivered_customer", "order_estimated_delivery_date": "time_estimated_delivery"})
    dim_time["time_key"] = pd.to_datetime(dim_time["time_key"], format='%Y-%m-%d %H:%M:%S')
    dim_time["time_approved"] = pd.to_datetime(dim_time["time_approved"], format='%Y-%m-%d %H:%M:%S')
    dim_time["time_delivered_carrier"] = pd.to_datetime(dim_time["time_delivered_carrier"], format='%Y-%m-%d %H:%M:%S')
    dim_time["time_delivered_customer"] = pd.to_datetime(dim_time["time_delivered_customer"], format='%Y-%m-%d %H:%M:%S')
    dim_time["time_estimated_delivery"] = pd.to_datetime(dim_time["time_estimated_delivery"], format='%Y-%m-%d %H:%M:%S')
    dim_time["time_shipping_limit"] = pd.to_datetime(dim_time["time_shipping_limit"], format='%Y-%m-%d %H:%M:%S')

    # --- Fact Table ---
    fact_orders = df_orders[["order_id", "order_status", "order_purchase_timestamp"]]
    fact_orders["order_purchase_timestamp"] = pd.to_datetime(fact_orders["order_purchase_timestamp"], format='%Y-%m-%d %H:%M:%S')
    fact_orders = fact_orders.merge(df_order_items[["order_id", "order_item_id", "price", "freight_value"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_review[["order_id", "review_key"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_customer[["order_id", "customer_key", "customer_zip_code_prefix"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_payment[["payment_key", "order_id"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_product[["product_key", "order_id"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_seller[["seller_key", "order_id", "seller_zip_code_prefix"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_time[["time_key"]], left_on="order_purchase_timestamp", right_on="time_key", how="left")
    fact_orders = fact_orders.rename(columns={"customer_zip_code_prefix": "geography_customer_key", "seller_zip_code_prefix": "geography_seller_key", "order_status": "status"})
    fact_orders = fact_orders.drop(columns=["order_purchase_timestamp"])
    fact_orders.drop_duplicates(inplace=True)
    fact_orders = fact_orders[["order_id", "order_item_id", "customer_key", "payment_key", "review_key", "product_key", "seller_key", "geography_customer_key", "geography_seller_key", "time_key", "status", "price", "freight_value"]]

    dim_customer.drop(columns=["order_id", "customer_zip_code_prefix"], inplace=True)
    dim_payment.drop(columns=["order_id"], inplace=True)
    dim_review.drop(columns=["order_id"], inplace=True)
    dim_product.drop(columns=["order_id"], inplace=True)
    dim_seller.drop(columns=["order_id", "seller_zip_code_prefix"], inplace=True)

    print("Transformación completa.")
    return dim_customer, dim_payment, dim_review, dim_product, dim_seller, dim_geography, dim_time, fact_orders


# =====================================
# LOAD
# =====================================
@task(name="Load to S3 (append mode)")
def load_to_s3(df, table_name, key_column):
    object_key = f"staging/{table_name}.parquet"

    try:
        response = s3.get_object(Bucket=os.getenv("S3_BUCKET"), Key=object_key)
        existing_df = pd.read_parquet(io.BytesIO(response["Body"].read()))
        print(f"{table_name}: {len(existing_df)} filas existentes en S3.")
    except s3.exceptions.NoSuchKey:
        existing_df = pd.DataFrame()
        print(f"{table_name}: no existe archivo previo en S3, se creará uno nuevo.")
    except Exception as e:
        print(f"Error leyendo {table_name} desde S3, se iniciará vacío: {e}")
        existing_df = pd.DataFrame()

    # Convertir claves UUID y columnas object a string (asegura consistencia)
    if 'payment_key' in df.columns:
        df['payment_key'] = df['payment_key'].astype(str)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)

    # Concatenar nuevos datos con los existentes
    merged = pd.concat([existing_df, df], ignore_index=True)

    # Eliminar duplicados exactos (toda la fila)
    merged.drop_duplicates(inplace=True)

    # Reescribir Parquet (siempre completo, pero sin reemplazar la data previa lógicamente)
    buffer = io.BytesIO()
    merged.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Subir el Parquet consolidado a S3
    s3.put_object(Bucket=os.getenv("S3_BUCKET"), Key=object_key, Body=buffer.getvalue())

    print(f"{table_name} actualizado → {len(merged)} filas totales (se agregaron {len(merged) - len(existing_df)} nuevas).")


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