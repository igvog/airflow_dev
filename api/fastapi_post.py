from fastapi import FastAPI, Query
import pandas as pd

app = FastAPI()

fact_order_items = pd.read_csv("data/olist_order_items_dataset.csv")
dim_order = pd.read_csv("data/olist_orders_dataset.csv")
dim_customer = pd.read_csv("data/olist_customers_dataset.csv")
dim_seller = pd.read_csv("data/olist_sellers_dataset.csv")

@app.get("/")
def root():
    return {"message": "API is running"}

@app.get("/order_items")
def get_order_items(limit: int = Query(100, ge=1), offset: int = Query(0, ge=0)):
    """Return paginated order_items"""
    subset = fact_order_items.iloc[offset:offset + limit]
    return subset.to_dict(orient="records")

@app.get("/customers")
def get_customers(limit: int = 100, offset: int = 0):
    """Return paginated customers"""
    subset = dim_customer.iloc[offset:offset + limit]
    return subset.to_dict(orient="records")

@app.get("/orders")
def get_orders(limit: int = Query(100, ge=1), offset: int = Query(0, ge=0)):
    """Return paginated orders"""
    subset = dim_order.iloc[offset:offset + limit]
    # Replace NaN with None for JSON serialization
    subset = subset.where(pd.notnull(subset), None)
    return subset.to_dict(orient="records")

@app.get("/sellers")
def get_sellers(limit: int = Query(100, ge=1), offset: int = Query(0, ge=0)):
    """Return paginated sellers"""
    subset = dim_seller.iloc[offset:offset + limit]
    return subset.to_dict(orient="records")
