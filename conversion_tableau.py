import pandas as pd

df = pd.read_parquet("../E-Commerce DataWarehouse/Staging/dim_customer.parquet")
df.to_csv("../E-Commerce DataWarehouse/Staging/dim_customer.csv", index=False)

df = pd.read_parquet("../E-Commerce DataWarehouse/Staging/dim_product.parquet")
df.to_csv("../E-Commerce DataWarehouse/Staging/dim_product.csv", index=False)

df = pd.read_parquet("../E-Commerce DataWarehouse/Staging/fact_orders.parquet")
df["geography_seller_key"] = df["geography_seller_key"].astype("Int64")
df.to_csv("../E-Commerce DataWarehouse/Staging/fact_orders.csv", index=False)

df = pd.read_parquet("../E-Commerce DataWarehouse/Staging/dim_time.parquet")
df.to_csv("../E-Commerce DataWarehouse/Staging/dim_time.csv", index=False)

df = pd.read_parquet("../E-Commerce DataWarehouse/Staging/dim_seller.parquet")
df.to_csv("../E-Commerce DataWarehouse/Staging/dim_seller.csv", index=False)

df = pd.read_parquet("../E-Commerce DataWarehouse/Staging/dim_geography.parquet")
df.to_csv("../E-Commerce DataWarehouse/Staging/dim_geography.csv", index=False)

df = pd.read_parquet("../E-Commerce DataWarehouse/Staging/dim_review.parquet")
df.to_csv("../E-Commerce DataWarehouse/Staging/dim_review.csv", index=False)

df = pd.read_parquet("../E-Commerce DataWarehouse/Staging/dim_payment.parquet")
df.to_csv("../E-Commerce DataWarehouse/Staging/dim_payment.csv", index=False)