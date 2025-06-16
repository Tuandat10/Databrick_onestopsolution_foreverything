import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Setup
fake = Faker()
Faker.seed(0)
np.random.seed(0)
output_dir = "ecommerce_parquet_ver4"
os.makedirs(output_dir, exist_ok=True)

# -------------------
# DimDate
# -------------------
start_date = datetime(2024, 1, 1)
dates = [start_date + timedelta(days=i) for i in range(1000)]
dim_date = pd.DataFrame({
    "DateKey": [d.strftime('%Y%m%d') for d in dates],
    "Date": [d.strftime('%Y-%m-%d') for d in dates],
    "DayOfWeek": [d.strftime('%A') for d in dates],
    "Month": [d.strftime('%b') for d in dates],
    "Quarter": [f"Q{((d.month - 1) // 3) + 1}" for d in dates],
    "Year": [d.year for d in dates],
    "HolidayFlag": [random.choice(['Yes', 'No']) for _ in range(1000)]
})
dim_date.to_parquet(f"{output_dir}/DimDate.parquet", index=False)

# -------------------
# DimCustomer
# -------------------
dim_customer = pd.DataFrame({
    "CustomerKey": range(101, 1101),
    "Name": [fake.name() for _ in range(1000)],
    "Gender": [random.choice(['M', 'F']) for _ in range(1000)],
    "Age": np.random.randint(18, 70, size=1000),
    "Country": [fake.country() for _ in range(1000)]
})
dim_customer.to_parquet(f"{output_dir}/DimCustomer.parquet", index=False)

# -------------------
# DimProduct
# -------------------
dim_product = pd.DataFrame({
    "ProductKey": range(501, 1501),
    "ProductName": [fake.word().capitalize() for _ in range(1000)],
    "Category": [random.choice(['Electronics', 'Home', 'Fashion', 
'Books']) for _ in range(1000)],
    "Brand": [fake.company() for _ in range(1000)],
    "UnitCost": np.round(np.random.uniform(5, 500, 1000), 2),
    "LaunchDate": [fake.date_between(start_date='-3y', end_date='today') 
for _ in range(1000)]
})
dim_product.to_parquet(f"{output_dir}/DimProduct.parquet", index=False)

# -------------------
# DimPaymentMethod
# -------------------
payment_methods = ['Credit Card', 'PayPal', 'Afterpay', 'Bank Transfer']
providers = ['Visa', 'PayPal', 'Afterpay', 'NAB']
dim_payment = pd.DataFrame({
    "PaymentKey": range(1, 1001),
    "PaymentType": [random.choice(payment_methods) for _ in range(1000)],
    "Provider": [random.choice(providers) for _ in range(1000)],
    "IsDigitalWallet": [random.choice(['Yes', 'No']) for _ in range(1000)]
})
dim_payment.to_parquet(f"{output_dir}/DimPaymentMethod.parquet", 
index=False)

# -------------------
# DimShippingMethod
# -------------------
shipping_methods = ['Standard', 'Express', 'Free Shipping']
carriers = ['FedEx', 'DHL', 'Australia Post']
dim_shipping = pd.DataFrame({
    "ShippingKey": range(1, 1001),
    "Method": [random.choice(shipping_methods) for _ in range(1000)],
    "Cost": np.round(np.random.uniform(0, 20, 1000), 2),
    "EstimatedDays": np.random.randint(1, 10, 1000),
    "Carrier": [random.choice(carriers) for _ in range(1000)]
})
dim_shipping.to_parquet(f"{output_dir}/DimShippingMethod.parquet", 
index=False)

# -------------------
# DimPromotion
# -------------------
promo_codes = ['NEWYEAR25', 'FREESHIP', 'WINTER10', 'SUMMER15']
dim_promo = pd.DataFrame({
    "PromotionKey": range(1, 1001),
    "PromoCode": [random.choice(promo_codes) for _ in range(1000)],
    "DiscountPercent": np.random.randint(5, 50, 1000),
    "StartDate": [fake.date_between(start_date='-1y', end_date='today') 
for _ in range(1000)],
    "EndDate": [fake.date_between(start_date='today', end_date='+30d') for 
_ in range(1000)]
})
dim_promo.to_parquet(f"{output_dir}/DimPromotion.parquet", index=False)

# -------------------
# DimRegion
# -------------------
dim_region = pd.DataFrame({
    "RegionKey": range(1, 1001),
    "Country": [fake.country() for _ in range(1000)],
    "State": [fake.state() for _ in range(1000)],
    "City": [fake.city() for _ in range(1000)],
    "EconomicZone": [random.choice(['APAC', 'EU', 'North America']) for _ 
in range(1000)]
})
dim_region.to_parquet(f"{output_dir}/DimRegion.parquet", index=False)

# -------------------
# FactSales
# -------------------
fact_sales = pd.DataFrame({
    "SalesKey": range(1, 1001),
    "DateKey": np.random.choice(dim_date["DateKey"], 1000),
    "CustomerKey": np.random.choice(dim_customer["CustomerKey"], 1000),
    "ProductKey": np.random.choice(dim_product["ProductKey"], 1000),
    "PaymentKey": np.random.choice(dim_payment["PaymentKey"], 1000),
    "ShippingKey": np.random.choice(dim_shipping["ShippingKey"], 1000),
    "PromotionKey": np.random.choice(dim_promo["PromotionKey"], 1000),
    "RegionKey": np.random.choice(dim_region["RegionKey"], 1000),
    "Quantity": np.random.randint(1, 5, size=1000),
    "UnitPrice": np.round(np.random.uniform(10, 500, 1000), 2)
})
fact_sales["TotalRevenue"] = fact_sales["Quantity"] * fact_sales["UnitPrice"]
fact_sales.to_parquet(f"{output_dir}/FactSales.parquet", index=False)

