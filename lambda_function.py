import os
import io
import uuid
import boto3
import pandas as pd
import sqlalchemy
import warnings

warnings.filterwarnings("ignore")

# ----------------------------
# Configuración global S3
# ----------------------------
s3 = boto3.client("s3")
S3_BUCKET = os.environ["S3_BUCKET"]

# ----------------------------
# Helpers de conexión
# ----------------------------
def get_engine():
    user = os.environ["PG_USER"]
    pwd = os.environ["PG_PASS"]
    host = os.environ["PG_HOST"]
    port = os.environ.get("PG_PORT", "5432")
    db   = os.environ["PG_DB"]
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    # Pool chico para Lambda
    return sqlalchemy.create_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True
    )

# ----------------------------
# EXTRACT (por chunks)
# ----------------------------
def extract_table_chunked(engine, table_name: str, chunk_size: int = 50000, max_rows: int | None = None) -> pd.DataFrame:
    """
    Lee una tabla por chunks con LIMIT/OFFSET y concatena.
    Si max_rows se especifica, deja de extraer cuando alcance ese máximo.
    """
    total_gathered = 0
    offset = 0
    frames = []

    while True:
        remaining = None if max_rows is None else max_rows - total_gathered
        this_chunk = chunk_size if remaining is None else max(0, min(chunk_size, remaining))
        if this_chunk == 0:
            break

        query = f"SELECT * FROM {table_name} ORDER BY 1 LIMIT {this_chunk} OFFSET {offset}"
        df_chunk = pd.read_sql(query, engine)

        if df_chunk.empty:
            break

        frames.append(df_chunk)
        got = len(df_chunk)
        total_gathered += got
        offset += got

        print(f"[{table_name}] +{got} filas (acumuladas: {total_gathered})")

        if max_rows is not None and total_gathered >= max_rows:
            break

    if frames:
        out = pd.concat(frames, ignore_index=True)
    else:
        out = pd.DataFrame()

    print(f"{len(out)} filas extraídas de {table_name}")
    return out

# ----------------------------
# TRANSFORM (idéntica a tu lógica)
# ----------------------------
def transform_data(
    df_customer,
    df_geolocation,
    df_order_items,
    df_order_payments,
    df_order_reviews,
    df_orders,
    df_products,
    df_sellers
):
    # --- Dimensión Customer ---
    dim_customer = df_customer[["customer_id", "customer_unique_id", "customer_zip_code_prefix"]].copy()
    dim_customer = dim_customer.merge(df_orders[["customer_id", "order_id"]], on="customer_id", how="left")
    dim_customer = dim_customer.rename(columns={"customer_id": "customer_key"})

    # --- Dimensión Payment ---
    dim_payment = df_order_payments[
        ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
    ].copy()
    # Generar payment_key como STRING para evitar ArrowTypeError
    dim_payment["payment_key"] = [str(uuid.uuid4()) for _ in range(len(dim_payment))]
    dim_payment = dim_payment[
        ["payment_key", "payment_sequential", "payment_type", "payment_installments", "payment_value", "order_id"]
    ]

    # --- Dimensión Review ---
    dim_review = df_order_reviews[
        ["review_id", "review_score", "review_comment_title", "review_comment_message",
         "review_creation_date", "review_answer_timestamp", "order_id"]
    ].copy()
    dim_review = dim_review.rename(columns={"review_id": "review_key", "review_answer_timestamp": "review_answer_date"})
    dim_review["review_creation_date"] = pd.to_datetime(dim_review["review_creation_date"], errors="coerce")
    dim_review["review_answer_date"] = pd.to_datetime(dim_review["review_answer_date"], errors="coerce")

    # --- Dimensión Product ---
    dim_product = df_products[
        ["product_id", "product_category_name", "product_name_length", "product_description_length",
         "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    ].copy()
    dim_product = dim_product.merge(df_order_items[["product_id", "order_id"]], on="product_id", how="left")
    dim_product = dim_product.rename(columns={
        "product_id": "product_key",
        "product_weight_g": "product_weight",
        "product_length_cm": "product_length",
        "product_height_cm": "product_height",
        "product_width_cm": "product_width"
    })

    # --- Dimensión Seller ---
    dim_seller = df_sellers[["seller_id", "seller_zip_code_prefix"]].copy()
    dim_seller = dim_seller.merge(df_order_items[["seller_id", "order_id"]], on="seller_id", how="left")
    dim_seller = dim_seller.rename(columns={"seller_id": "seller_key"})

    # --- Dimensión Geography ---
    dim_geography = df_geolocation[
        ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"]
    ].copy()
    dim_geography = dim_geography.rename(columns={
        "geolocation_zip_code_prefix": "geography_key",
        "geolocation_lat": "geography_lat",
        "geolocation_lng": "geography_lng",
        "geolocation_city": "geography_city",
        "geolocation_state": "geography_state"
    })

    # --- Dimensión Time ---
    dim_time = df_orders[
        ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
         "order_delivered_customer_date", "order_estimated_delivery_date"]
    ].copy()
    # Nota: esta línea asume alineación con df_order_items como en tu código original
    dim_time["time_shipping_limit"] = df_order_items["shipping_limit_date"]
    dim_time = dim_time.rename(columns={
        "order_purchase_timestamp": "time_key",
        "order_approved_at": "time_approved",
        "order_delivered_carrier_date": "time_delivered_carrier",
        "order_delivered_customer_date": "time_delivered_customer",
        "order_estimated_delivery_date": "time_estimated_delivery"
    })
    for col in ["time_key", "time_approved", "time_delivered_carrier", "time_delivered_customer",
                "time_estimated_delivery", "time_shipping_limit"]:
        dim_time[col] = pd.to_datetime(dim_time[col], errors="coerce")

    # --- Fact Table ---
    fact_orders = df_orders[["order_id", "order_status", "order_purchase_timestamp"]].copy()
    fact_orders["order_purchase_timestamp"] = pd.to_datetime(fact_orders["order_purchase_timestamp"], errors="coerce")
    fact_orders = fact_orders.merge(
        df_order_items[["order_id", "order_item_id", "price", "freight_value"]],
        on="order_id", how="left"
    )
    fact_orders = fact_orders.merge(dim_review[["order_id", "review_key"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(
        dim_customer[["order_id", "customer_key", "customer_zip_code_prefix"]],
        on="order_id", how="left"
    )
    fact_orders = fact_orders.merge(dim_payment[["payment_key", "order_id"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(dim_product[["product_key", "order_id"]], on="order_id", how="left")
    fact_orders = fact_orders.merge(
        dim_seller[["seller_key", "order_id", "seller_zip_code_prefix"]],
        on="order_id", how="left"
    )
    fact_orders = fact_orders.merge(
        dim_time[["time_key"]],
        left_on="order_purchase_timestamp", right_on="time_key", how="left"
    )
    fact_orders = fact_orders.rename(columns={
        "customer_zip_code_prefix": "geography_customer_key",
        "seller_zip_code_prefix": "geography_seller_key",
        "order_status": "status"
    })
    fact_orders = fact_orders.drop(columns=["order_purchase_timestamp"])
    fact_orders.drop_duplicates(inplace=True)
    fact_orders = fact_orders[
        ["order_id", "order_item_id", "customer_key", "payment_key", "review_key", "product_key",
         "seller_key", "geography_customer_key", "geography_seller_key", "time_key", "status", "price", "freight_value"]
    ]

    # Limpiar columnas intermedias según tu lógica original
    dim_customer = dim_customer.drop(columns=["order_id", "customer_zip_code_prefix"])
    dim_payment  = dim_payment.drop(columns=["order_id"])
    dim_review   = dim_review.drop(columns=["order_id"])
    dim_product  = dim_product.drop(columns=["order_id"])
    dim_seller   = dim_seller.drop(columns=["order_id", "seller_zip_code_prefix"])

    print("Transformación completa.")
    return dim_customer, dim_payment, dim_review, dim_product, dim_seller, dim_geography, dim_time, fact_orders

# ----------------------------
# LOAD (incremental a S3 Parquet)
# ----------------------------
def load_to_s3_incremental(df: pd.DataFrame, table_name: str, key_column: str):
    object_key = f"staging/{table_name}.parquet"

    # Forzar object->string (evitar problemas con Arrow)
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str)

    # Intentar leer existente
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=object_key)
        existing = pd.read_parquet(io.BytesIO(resp["Body"].read()))
        # Homogeneizar dtypes básicos
        for col in df.columns:
            if col in existing.columns:
                try:
                    df[col] = df[col].astype(existing[col].dtype)
                except Exception:
                    pass
        merged = pd.concat([existing, df], ignore_index=True)
        merged = merged.drop_duplicates(subset=[key_column])
    except s3.exceptions.NoSuchKey:
        merged = df.copy()

    # Escribir Parquet
    buf = io.BytesIO()
    merged.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=object_key, Body=buf.getvalue())

    print(f"{table_name} actualizado → {len(merged)} registros totales en S3.")

# ----------------------------
# Handler de AWS Lambda
# ----------------------------
def handler(event, context):
    """
    event opcional:
      {
        "chunk_size": 50000,
        "max_rows_per_table": null   # o un entero para limitar lectura por tabla
      }
    """
    chunk_size = int(event.get("chunk_size", 50000)) if isinstance(event, dict) else 50000
    max_rows   = event.get("max_rows_per_table") if isinstance(event, dict) else None
    if isinstance(max_rows, str) and max_rows.isdigit():
        max_rows = int(max_rows)

    engine = get_engine()
    try:
        print(f"Iniciando ETL (chunk_size={chunk_size}, max_rows={max_rows})")

        # -------- EXTRACT --------
        df_customer      = extract_table_chunked(engine, "customers",       chunk_size, max_rows)
        df_geolocation   = extract_table_chunked(engine, "geolocation",     chunk_size, max_rows)
        df_order_items   = extract_table_chunked(engine, "order_items",     chunk_size, max_rows)
        df_order_payments= extract_table_chunked(engine, "order_payments",  chunk_size, max_rows)
        df_order_reviews = extract_table_chunked(engine, "order_reviews",   chunk_size, max_rows)
        df_orders        = extract_table_chunked(engine, "orders",          chunk_size, max_rows)
        df_products      = extract_table_chunked(engine, "products",        chunk_size, max_rows)
        df_sellers       = extract_table_chunked(engine, "sellers",         chunk_size, max_rows)

        # -------- TRANSFORM --------
        (dim_customer, dim_payment, dim_review, dim_product,
         dim_seller, dim_geo, dim_time, fact_orders) = transform_data(
            df_customer, df_geolocation, df_order_items, df_order_payments,
            df_order_reviews, df_orders, df_products, df_sellers
        )

        # -------- LOAD (incremental) --------
        load_to_s3_incremental(dim_customer, "dim_customer", "customer_key")
        load_to_s3_incremental(dim_payment,  "dim_payment",  "payment_key")
        load_to_s3_incremental(dim_review,   "dim_review",   "review_key")
        load_to_s3_incremental(dim_product,  "dim_product",  "product_key")
        load_to_s3_incremental(dim_seller,   "dim_seller",   "seller_key")
        load_to_s3_incremental(dim_geo,      "dim_geography","geography_key")
        load_to_s3_incremental(dim_time,     "dim_time",     "time_key")
        load_to_s3_incremental(fact_orders,  "fact_orders",  "order_id")

        print("ETL completo y datos cargados a zona de staging (S3).")
        return {"status": "ok"}

    finally:
        try:
            engine.dispose()
        except Exception:
            pass
