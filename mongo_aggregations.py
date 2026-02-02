

from pymongo import MongoClient

# Connect to local MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["e_commerce"]


def revenue_by_category(limit=20):
    pipeline = [
        {"$unwind": "$items"},
        {
            "$lookup": {
                "from": "products",
                "localField": "items.product_id",
                "foreignField": "product_id",
                "as": "product",
            }
        },
        {"$unwind": "$product"},
        {
            "$group": {
                "_id": "$product.category_id",
                "totalRevenue": {"$sum": "$items.subtotal"},
                "totalQuantity": {"$sum": "$items.quantity"},
                "orderCount": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "category_id": "$_id",
                "totalRevenue": 1,
                "totalQuantity": 1,
                "orderCount": 1,
            }
        },
        {"$sort": {"totalRevenue": -1}},
        {"$limit": limit},
    ]

    print("\n=== Revenue by Category ===")
    for doc in db.transactions.aggregate(pipeline):
        print(doc)


def user_segmentation():
    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "orders": {"$sum": 1},
                "totalSpent": {"$sum": "$total"},
            }
        },
        {
            "$addFields": {
                "segment": {
                    "$switch": {
                        "branches": [
                            {"case": {"$lt": ["$orders", 3]}, "then": "Low"},
                            {
                                "case": {
                                    "$and": [
                                        {"$gte": ["$orders", 3]},
                                        {"$lt": ["$orders", 7]},
                                    ]
                                },
                                "then": "Medium",
                            },
                        ],
                        "default": "High",
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$segment",
                "customers": {"$sum": 1},
                "avgOrders": {"$avg": "$orders"},
                "avgSpent": {"$avg": "$totalSpent"},
                "totalRevenue": {"$sum": "$totalSpent"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "segment": "$_id",
                "customers": 1,
                "avgOrders": 1,
                "avgSpent": 1,
                "totalRevenue": 1,
            }
        },
        {"$sort": {"totalRevenue": -1}},
    ]

    print("\n=== User Segmentation (Low / Medium / High) ===")
    for doc in db.transactions.aggregate(pipeline):
        print(doc)


def top_selling_products(limit=10):
    pipeline = [
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.product_id",
                "totalQuantity": {"$sum": "$items.quantity"},
                "totalRevenue": {"$sum": "$items.subtotal"},
                "orderCount": {"$sum": 1},
            }
        },
        {
            "$lookup": {
                "from": "products",
                "localField": "_id",
                "foreignField": "product_id",
                "as": "product",
            }
        },
        {"$unwind": "$product"},
        {
            "$project": {
                "_id": 0,
                "product_id": "$_id",
                "name": "$product.name",
                "totalQuantity": 1,
                "totalRevenue": 1,
                "orderCount": 1,
            }
        },
        {"$sort": {"totalQuantity": -1}},
        {"$limit": limit},
    ]

    print(f"\n=== Top {limit} Selling Products ===")
    for doc in db.transactions.aggregate(pipeline):
        print(doc)


if __name__ == "__main__":
    # Quick sanity check on counts
    print("Counts:")
    print("  users       :", db.users.count_documents({}))
    print("  products    :", db.products.count_documents({}))
    print("  categories  :", db.categories.count_documents({}))
    print("  sessions    :", db.sessions.count_documents({}))
    print("  transactions:", db.transactions.count_documents({}))

    revenue_by_category()
    user_segmentation()
    top_selling_products()