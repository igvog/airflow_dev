from typing import TypedDict, Optional

class Customer(TypedDict):
    customer_id: str
    customer_unique_id: str
    customer_zip_code_prefix: str
    customer_city: str
    customer_state: str

class Order(TypedDict):
    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: str
    order_approved_at: Optional[str]
    order_delivered_carrier_date: Optional[str]
    order_delivered_customer_date: Optional[str]
    order_estimated_delivery_date: str

class Seller(TypedDict):
    seller_id: str
    seller_zip_code_prefix: str
    seller_city: str
    seller_state: str

class OrderItem(TypedDict):
    order_id: str
    order_item_id: str
    product_id: str
    seller_id: str
    shipping_limit_date: str
    price: str
    freight_value: str