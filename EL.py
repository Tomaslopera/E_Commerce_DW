import uuid
import pandas as pd
import sqlalchemy
import sqlalchemy.dialects.postgresql as pg

import sqlite3
conn = sqlite3.connect('olist.sqlite')

# --- DATA EXTRACTION ---
def extract_data(table):
    query = f"SELECT * FROM {table}"
    return pd.read_sql_query(query, conn)

df_customer = extract_data("customers")
df_geolocation = extract_data("geolocation")
df_order_items = extract_data("order_items")
df_order_payments = extract_data("order_payments")
df_order_reviews = extract_data("order_reviews")
df_orders = extract_data("orders")
df_products = extract_data("products")
df_sellers = extract_data("sellers")

# --- DATA TRANSFORMATION ---
df_products = df_products.rename(columns={"product_name_lenght": "product_name_length", "product_description_lenght": "product_description_length"})
df_order_reviews = df_order_reviews.drop_duplicates(subset=['order_id'], keep='first')

# --- SCHEMA DEFINITIONS ---
customers_dtype = {
    "customer_id": pg.TEXT(),
    "customer_unique_id": pg.TEXT(),
    "customer_zip_code_prefix": pg.INTEGER(),
    "customer_city": pg.TEXT(),
    "customer_state": pg.TEXT(),
}

geolocation_dtype = {
    "geolocation_zip_code_prefix": pg.INTEGER(),
    "geolocation_lat": pg.FLOAT(),
    "geolocation_lng": pg.FLOAT(),
    "geolocation_city": pg.TEXT(),
    "geolocation_state": pg.TEXT(),
}

sellers_dtype = {
    "seller_id": pg.TEXT(),
    "seller_zip_code_prefix": pg.INTEGER(),
    "seller_city": pg.TEXT(),
    "seller_state": pg.TEXT(),
}

products_dtype = {
    "product_id": pg.TEXT(),
    "product_category_name": pg.TEXT(),
    "product_name_length": pg.INTEGER(),
    "product_description_length": pg.INTEGER(),
    "product_photos_qty": pg.INTEGER(),
    "product_weight_g": pg.INTEGER(),
    "product_length_cm": pg.INTEGER(),
    "product_height_cm": pg.INTEGER(),
    "product_width_cm": pg.INTEGER(),
}

translation_dtype = {
    "product_category_name": pg.TEXT(),
    "product_category_name_english": pg.TEXT(),
}

orders_dtype = {
    "order_id": pg.TEXT(),
    "customer_id": pg.TEXT(),
    "order_status": pg.TEXT(),
    "order_purchase_timestamp": pg.TIMESTAMP(),
    "order_approved_at": pg.TIMESTAMP(),
    "order_delivered_carrier_date": pg.TIMESTAMP(),
    "order_delivered_customer_date": pg.TIMESTAMP(),
    "order_estimated_delivery_date": pg.TIMESTAMP(),
}

order_items_dtype = {
    "order_id": pg.TEXT(),
    "order_item_id": pg.INTEGER(),
    "product_id": pg.TEXT(),
    "seller_id": pg.TEXT(),
    "shipping_limit_date": pg.TIMESTAMP(),
    "price": pg.NUMERIC(10, 2),
    "freight_value": pg.NUMERIC(10, 2),
}

order_payments_dtype = {
    "order_id": pg.TEXT(),
    "payment_sequential": pg.INTEGER(),
    "payment_type": pg.TEXT(),
    "payment_installments": pg.INTEGER(),
    "payment_value": pg.NUMERIC(10, 2),
}

order_reviews_dtype = {
    "review_id": pg.TEXT(),
    "order_id": pg.TEXT(),
    "review_score": pg.INTEGER(),
    "review_comment_title": pg.TEXT(),
    "review_comment_message": pg.TEXT(),
    "review_creation_date": pg.TIMESTAMP(),
    "review_answer_timestamp": pg.TIMESTAMP(),
}

# --- DATA INSERTION ---
engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:root@localhost:5432/E_Commerce')

with engine.begin() as conn:
    df_customer.to_sql("customers", conn, if_exists="append", index=False, dtype=customers_dtype)
    df_geolocation.to_sql("geolocation", conn, if_exists="append", index=False, dtype=geolocation_dtype)
    df_sellers.to_sql("sellers", conn, if_exists="append", index=False, dtype=sellers_dtype)
    df_products.to_sql("products", conn, if_exists="append", index=False, dtype=products_dtype)
    df_orders.to_sql("orders", conn, if_exists="append", index=False, dtype=orders_dtype)
    df_order_items.to_sql("order_items", conn, if_exists="append", index=False, dtype=order_items_dtype)
    df_order_payments.to_sql("order_payments", conn, if_exists="append", index=False, dtype=order_payments_dtype)
    df_order_reviews.to_sql("order_reviews", conn, if_exists="replace", index=False, dtype=order_reviews_dtype)

    print("Inserción completada con éxito en todas las tablas.")