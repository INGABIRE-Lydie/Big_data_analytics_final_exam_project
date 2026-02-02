#!/usr/bin/env python3
"""
dataset_generator.py

Generates synthetic e-commerce data for AUCA Big Data Analytics project.

Outputs (JSON Lines: one JSON object per line):
- users.json
- categories.json
- products.json
- sessions.json
- transactions.json
"""

import json
import random
import datetime as dt
import uuid
import threading
from typing import List, Dict, Any, Optional

from faker import Faker

# ---------------- CONFIG ----------------
# You can lower these numbers while developing and increase later.
NUM_USERS = 5000
NUM_PRODUCTS = 2000
NUM_CATEGORIES = 25
NUM_TRANSACTIONS = 100000      # target / max transactions
NUM_SESSIONS = 300000
TIMESPAN_DAYS = 90

fake = Faker()

# ---------------- INIT (reproducibility) ----------------
random.seed(42)
Faker.seed(42)

# ---------------- ID HELPERS ----------------
def generate_session_id() -> str:
    return f"sess_{uuid.uuid4().hex[:10]}"

def generate_transaction_id() -> str:
    return f"txn_{uuid.uuid4().hex[:12]}"

# ---------------- INVENTORY ----------------
class InventoryManager:
    """
    Simple inventory manager with a lock to avoid race conditions.
    """
    def __init__(self, products: List[Dict[str, Any]]):
        # Map product_id -> product dict
        self.products: Dict[str, Dict[str, Any]] = {
            p["product_id"]: p for p in products
        }
        self.lock = threading.RLock()

    def update_stock(self, product_id: str, qty: int) -> bool:
        """
        Try to reduce stock for product_id by qty.
        Returns True on success, False if insufficient stock or product not found.
        """
        with self.lock:
            product = self.products.get(product_id)
            if not product:
                return False
            if product["current_stock"] >= qty:
                product["current_stock"] -= qty
                return True
            return False

    def get_product(self, product_id: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            return self.products.get(product_id)

# ---------------- PAGE FLOW ----------------
def determine_page_type(pos: int, pages: List[Dict[str, Any]]) -> str:
    """
    Decide next page type in a session, using simple Markov-like transitions.
    """
    if pos == 0 or not pages:
        return random.choice(["home", "search", "category_listing"])

    prev = pages[-1]["page_type"]

    flows = {
        "home": (
            ["category_listing", "search", "product_detail"],
            [0.5, 0.3, 0.2],
        ),
        "category_listing": (
            ["product_detail", "search", "category_listing", "home"],
            [0.6, 0.2, 0.1, 0.1],
        ),
        "search": (
            ["product_detail", "search", "category_listing", "home"],
            [0.6, 0.2, 0.1, 0.1],
        ),
        "product_detail": (
            ["product_detail", "cart", "search", "home"],
            [0.3, 0.4, 0.15, 0.15],
        ),
        "cart": (
            ["checkout", "product_detail", "home"],
            [0.6, 0.2, 0.2],
        ),
        "checkout": (
            ["confirmation", "cart", "home"],
            [0.7, 0.1, 0.2],
        ),
        "confirmation": (
            ["home", "product_detail"],
            [0.7, 0.3],
        ),
    }

    options, weights = flows.get(prev, (["home"], [1.0]))
    return random.choices(options, weights)[0]

# ---------------- CONTENT ----------------
def get_page_content(
    page_type: str,
    products: List[Dict[str, Any]],
    categories: List[Dict[str, Any]],
    inventory: InventoryManager,
) -> (Optional[Dict[str, Any]], Optional[Dict[str, Any]]):
    """
    For product_detail: choose an active product with stock if possible.
    For category_listing: choose a random category.
    Others: no product/category.
    """
    if page_type == "product_detail":
        # Try up to 10 random picks to find an active product with stock
        for _ in range(10):
            p = random.choice(products)
            inv_p = inventory.get_product(p["product_id"])
            if inv_p and inv_p["is_active"] and inv_p["current_stock"] > 0:
                # find its category
                cat = next(
                    (c for c in categories if c["category_id"] == p["category_id"]),
                    None,
                )
                return p, cat
        # Fallback: any product, any category
        p = random.choice(products)
        cat = next(
            (c for c in categories if c["category_id"] == p["category_id"]),
            None,
        )
        return p, cat

    if page_type == "category_listing":
        cat = random.choice(categories)
        return None, cat

    return None, None

# ---------------- CATEGORIES ----------------
categories: List[Dict[str, Any]] = []
for i in range(NUM_CATEGORIES):
    cat_id = f"cat_{i:03d}"
    subcategories = []
    for j in range(random.randint(3, 5)):
        subcategories.append(
            {
                "subcategory_id": f"sub_{i:03d}_{j:02d}",
                "name": fake.bs().title(),
                "profit_margin": round(random.uniform(0.1, 0.4), 2),
            }
        )
    categories.append(
        {
            "category_id": cat_id,
            "name": fake.company(),
            "subcategories": subcategories,
        }
    )

print(f"Generated {len(categories)} categories")

# ---------------- PRODUCTS ----------------
products: List[Dict[str, Any]] = []

product_creation_start = dt.datetime.now() - dt.timedelta(days=TIMESPAN_DAYS * 2)

for i in range(NUM_PRODUCTS):
    category = random.choice(categories)
    subcategory = random.choice(category["subcategories"])

    # Price history: 1–3 price points
    base_price = round(random.uniform(5, 500), 2)
    price_history: List[Dict[str, Any]] = []

    initial_date = fake.date_time_between(
        start_date=product_creation_start,
        end_date=product_creation_start + dt.timedelta(days=TIMESPAN_DAYS // 3),
    )
    price_history.append(
        {"price": base_price, "date": initial_date.isoformat()}
    )

    for _ in range(random.randint(0, 2)):
        change_date = fake.date_time_between(
            start_date=initial_date, end_date="now"
        )
        new_price = round(base_price * random.uniform(0.8, 1.2), 2)
        price_history.append(
            {"price": new_price, "date": change_date.isoformat()}
        )
        initial_date = change_date

    price_history.sort(key=lambda x: x["date"])
    current_price = price_history[-1]["price"]

    products.append(
        {
            "product_id": f"prod_{i:05d}",
            "name": fake.catch_phrase().title(),
            "category_id": category["category_id"],
            "subcategory_id": subcategory["subcategory_id"],
            "base_price": current_price,  # treat as current/base price
            "current_stock": random.randint(10, 1000),
            "is_active": random.random() < 0.95,
            "price_history": price_history,
            "creation_date": price_history[0]["date"],
        }
    )

print(f"Generated {len(products)} products")

# ---------------- USERS ----------------
users: List[Dict[str, Any]] = []

for i in range(NUM_USERS):
    reg_date = fake.date_time_between(
        start_date=f"-{TIMESPAN_DAYS * 3}d",
        end_date=f"-{TIMESPAN_DAYS}d",
    )
    users.append(
        {
            "user_id": f"user_{i:06d}",
            "geo_data": {
                "city": fake.city(),
                "state": fake.state_abbr(),
                "country": fake.country_code(),
            },
            "registration_date": reg_date.isoformat(),
            "last_active": fake.date_time_between(
                start_date=reg_date, end_date="now"
            ).isoformat(),
        }
    )

print(f"Generated {len(users)} users")

# ---------------- SESSIONS + TRANSACTIONS ----------------
inventory = InventoryManager(products)
sessions: List[Dict[str, Any]] = []
transactions: List[Dict[str, Any]] = []

print("Generating sessions and linked transactions...")

for _ in range(NUM_SESSIONS):
    user = random.choice(users)
    session_id = generate_session_id()

    # Session start within the last TIMESPAN_DAYS
    session_start = fake.date_time_between(
        start_date=f"-{TIMESPAN_DAYS}d", end_date="now"
    )
    session_duration = random.randint(30, 3600)  # 30 sec to 1 hour
    session_end = session_start + dt.timedelta(seconds=session_duration)

    # Build page timeline: split duration into several segments
    num_views = random.randint(4, 15)
    breakpoints = sorted(
        [0]
        + [
            random.randint(1, session_duration - 1)
            for _ in range(num_views - 1)
        ]
        + [session_duration]
    )

    page_views: List[Dict[str, Any]] = []
    viewed_products = set()
    cart_contents: Dict[str, Dict[str, Any]] = {}

    for i in range(len(breakpoints) - 1):
        offset = breakpoints[i]
        view_duration = breakpoints[i + 1] - breakpoints[i]
        page_type = determine_page_type(i, page_views)

        product, category = get_page_content(
            page_type, products, categories, inventory
        )

        # Track views and possible cart additions
        if page_type == "product_detail" and product:
            pid = product["product_id"]
            viewed_products.add(pid)

            # 30% chance to add product to cart
            if random.random() < 0.3:
                if pid not in cart_contents:
                    cart_contents[pid] = {
                        "quantity": 0,
                        "price": product["base_price"],
                    }

                max_to_add = 3 - cart_contents[pid]["quantity"]
                if max_to_add > 0:
                    add_qty = random.randint(1, max_to_add)
                    cart_contents[pid]["quantity"] += add_qty

        page_views.append(
            {
                "timestamp": (
                    session_start + dt.timedelta(seconds=offset)
                ).isoformat(),
                "page_type": page_type,
                "product_id": product["product_id"] if product else None,
                "category_id": category["category_id"] if category else None,
                "view_duration": view_duration,
            }
        )

    # Determine conversion status
    has_checkout = any(
        pv["page_type"] in ("checkout", "confirmation")
        for pv in page_views
    )
    if cart_contents and has_checkout:
        converted = random.random() < 0.7  # 70% chance to convert
    else:
        converted = False

    if converted and len(transactions) < NUM_TRANSACTIONS:
        # Create a transaction from this session's cart_contents
        items: List[Dict[str, Any]] = []
        valid = True
        subtotal = 0.0

        for pid, details in cart_contents.items():
            qty = details["quantity"]
            if qty <= 0:
                continue
            if inventory.update_stock(pid, qty):
                line_subtotal = round(details["price"] * qty, 2)
                items.append(
                    {
                        "product_id": pid,
                        "quantity": qty,
                        "unit_price": details["price"],
                        "subtotal": line_subtotal,
                    }
                )
                subtotal += line_subtotal
            else:
                valid = False
                break

        if valid and items:
            discount = 0.0
            if random.random() < 0.2:
                discount_rate = random.choice([0.05, 0.1, 0.15, 0.2])
                discount = round(subtotal * discount_rate, 2)
            total = round(subtotal - discount, 2)

            transactions.append(
                {
                    "transaction_id": generate_transaction_id(),
                    "user_id": user["user_id"],
                    "session_id": session_id,
                    "timestamp": session_end.isoformat(),
                    "items": items,
                    "subtotal": round(subtotal, 2),
                    "discount": discount,
                    "total": total,
                    "payment_method": random.choice(
                        ["credit_card", "paypal", "bank_transfer", "gift_card"]
                    ),
                    "status": "completed",
                }
            )

    # Session geo from user + IP
    session_geo = user["geo_data"].copy()
    session_geo["ip_address"] = fake.ipv4()

    # Device profile
    device_profile = {
        "type": random.choice(["mobile", "desktop", "tablet"]),
        "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
        "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
    }

    conversion_status = (
        "converted"
        if converted
        else "abandoned"
        if cart_contents
        else "browsed"
    )

    sessions.append(
        {
            "session_id": session_id,
            "user_id": user["user_id"],
            "start_time": session_start.isoformat(),
            "end_time": session_end.isoformat(),
            "duration_seconds": session_duration,
            "geo_data": session_geo,
            "device_profile": device_profile,
            "viewed_products": list(viewed_products),
            "page_views": page_views,
            "cart_contents": {
                pid: details
                for pid, details in cart_contents.items()
                if details["quantity"] > 0
            },
            "conversion_status": conversion_status,
            "referrer": random.choice(
                ["direct", "email", "social", "search_engine", "affiliate"]
            ),
        }
    )

print(f"Generated {len(sessions)} sessions from which {len(transactions)} transactions were created")

# If we still haven't reached NUM_TRANSACTIONS, create extra transactions not tied to sessions
print("Generating additional standalone transactions (no session linkage)...")

while len(transactions) < NUM_TRANSACTIONS:
    user = random.choice(users)
    session_id = None
    timestamp = fake.date_time_between(
        start_date=f"-{TIMESPAN_DAYS}d", end_date="now"
    )

    items: List[Dict[str, Any]] = []
    subtotal = 0.0

    for _ in range(random.randint(1, 4)):
        product = random.choice(products)
        if not product["is_active"]:
            continue
        qty = random.randint(1, 3)
        if inventory.update_stock(product["product_id"], qty):
            line_subtotal = round(product["base_price"] * qty, 2)
            items.append(
                {
                    "product_id": product["product_id"],
                    "quantity": qty,
                    "unit_price": product["base_price"],
                    "subtotal": line_subtotal,
                }
            )
            subtotal += line_subtotal

    if not items:
        continue

    discount = 0.0
    if random.random() < 0.2:
        discount_rate = random.choice([0.05, 0.1, 0.15, 0.2])
        discount = round(subtotal * discount_rate, 2)
    total = round(subtotal - discount, 2)

    transactions.append(
        {
            "transaction_id": generate_transaction_id(),
            "user_id": user["user_id"],
            "session_id": session_id,
            "timestamp": timestamp.isoformat(),
            "items": items,
            "subtotal": round(subtotal, 2),
            "discount": discount,
            "total": total,
            "payment_method": random.choice(
                ["credit_card", "paypal", "bank_transfer", "gift_card"]
            ),
            "status": random.choice(
                ["completed", "processing", "shipped", "delivered"]
            ),
        }
    )

print(f"Total transactions generated: {len(transactions)}")

# ---------------- SAVE AS JSON LINES ----------------
def serializer(obj: Any):
    if isinstance(obj, (dt.date, dt.datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def write_json_lines(path: str, docs: List[Dict[str, Any]]):
    """Write a list of documents as JSON Lines: one JSON object per line."""
    with open(path, "w", encoding="utf-8") as f:
        for doc in docs:
            line = json.dumps(doc, default=serializer)
            f.write(line + "\n")

print("Saving datasets in JSON Lines format...")

write_json_lines("users.json", users)
write_json_lines("categories.json", categories)
# use inventory.products to reflect updated stock
write_json_lines("products.json", list(inventory.products.values()))
write_json_lines("sessions.json", sessions)
write_json_lines("transactions.json", transactions)

print("Dataset generation completed successfully.")
print(
    f"Summary:\n"
    f"  Users:        {len(users):,}\n"
    f"  Categories:   {len(categories):,}\n"
    f"  Products:     {len(products):,}\n"
    f"  Sessions:     {len(sessions):,}\n"
    f"  Transactions: {len(transactions):,}\n"
)