TABLE_CONFIG = {
    "raw_customers": {
        "keys": ["customer_id"],
        "surrogate": False,
        "unique_key": True,
    },
    "raw_geolocation": {
        "keys": ["surrogate_key"],
        "surrogate": True,
        "unique_key": False,
        # MD5 calculado a partir de todas as colunas da tabela
        "surrogate_columns": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ],
    },
    "raw_orders": {
        "keys": ["order_id"],
        "surrogate": False,
        "unique_key": True,
    },
    "raw_order_items": {
        "keys": ["order_id", "order_item_id"],
        "surrogate": False,
        "unique_key": True,
    },
    "raw_order_payments": {
        "keys": ["order_id", "payment_sequential"],
        "surrogate": False,
        "unique_key": True,
    },
    "raw_order_reviews": {
        "keys": ["review_id", "order_id"],
        "surrogate": False,
        "unique_key": True,
    },
    "raw_products": {
        "keys": ["product_id"],
        "surrogate": False,
        "unique_key": True,
    },
    "raw_sellers": {
        "keys": ["seller_id"],
        "surrogate": False,
        "unique_key": True,
    },
    "raw_product_category_name_translation": {
        "keys": ["product_category_name"],
        "surrogate": False,
        "unique_key": True,
    },
}

