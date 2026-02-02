import json
from collections import defaultdict, Counter
import math
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------------
# Helpers: load JSON Lines
# ---------------------------------------------------------------------------
def load_json_lines(path):
    data = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data.append(json.loads(line))
    return data

print("Loading data...")

users = load_json_lines("users.json")
products = load_json_lines("products.json")
categories = load_json_lines("categories.json")
transactions = load_json_lines("transactions.json")
sessions = load_json_lines("sessions.json")

print("Loaded:")
print("  users        :", len(users))
print("  products     :", len(products))
print("  categories   :", len(categories))
print("  transactions :", len(transactions))
print("  sessions     :", len(sessions))

# Build quick lookup maps
prod_to_cat = {p["product_id"]: p.get("category_id") for p in products}
user_geo = {u["user_id"]: u.get("geo_data", {}) for u in users}

# ---------------------------------------------------------------------------
# 1) Visualization: Revenue by category
# ---------------------------------------------------------------------------
print("Building Visualization 1: Revenue by category...")

rev_by_cat = defaultdict(float)
qty_by_cat = defaultdict(int)

for t in transactions:
    if t.get("status") != "completed":
        continue
    for item in t.get("items", []):
        if not isinstance(item, dict):
            continue
        pid = item.get("product_id")
        if not pid:
            continue
        cat = prod_to_cat.get(pid)
        if not cat:
            continue
        qty = item.get("quantity", 0) or 0
        sub = item.get("subtotal", 0.0) or 0.0
        try:
            qty = int(qty)
        except Exception:
            qty = 0
        try:
            sub = float(sub)
        except Exception:
            sub = 0.0
        qty_by_cat[cat] += qty
        rev_by_cat[cat] += sub

# Sort categories by revenue
sorted_cats = sorted(rev_by_cat.items(), key=lambda x: x[1], reverse=True)
top_n = 10
top_cats = sorted_cats[:top_n]
cat_ids = [c for c, _ in top_cats]
cat_revs = [rev_by_cat[c] for c in cat_ids]

plt.figure(figsize=(10, 6))
plt.bar(cat_ids, cat_revs, color="steelblue")
plt.title(f"Top {top_n} Categories by Revenue")
plt.xlabel("Category ID")
plt.ylabel("Total Revenue")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("viz_revenue_by_category.png")
plt.close()
print("Saved: viz_revenue_by_category.png")

# ---------------------------------------------------------------------------
# 2) Compute CLV + engagement metrics
# ---------------------------------------------------------------------------
print("Computing CLV and engagement metrics...")

# Session stats
sess_count = defaultdict(int)
sess_total_duration = defaultdict(float)

for s in sessions:
    uid = s.get("user_id")
    if not uid:
        continue
    sess_count[uid] += 1
    dur = s.get("duration_seconds", 0) or 0
    try:
        dur = float(dur)
    except Exception:
        dur = 0.0
    sess_total_duration[uid] += dur

# Spend stats (completed transactions)
orders_count = defaultdict(int)
total_spent = defaultdict(float)

for t in transactions:
    if t.get("status") != "completed":
        continue
    uid = t.get("user_id")
    if not uid:
        continue
    orders_count[uid] += 1
    tot = t.get("total", 0.0) or 0.0
    try:
        tot = float(tot)
    except Exception:
        tot = 0.0
    total_spent[uid] += tot

# Build combined CLV records
clv_records = []
for uid, scount in sess_count.items():
    avg_dur = sess_total_duration[uid] / scount if scount > 0 else 0.0
    orders = orders_count.get(uid, 0)
    spent = total_spent.get(uid, 0.0)
    geo = user_geo.get(uid, {})
    country = geo.get("country")

    # Segment by total spent
    if spent >= 500:
        seg = "high_value"
    elif spent >= 200:
        seg = "medium_value"
    elif spent == 0:
        seg = "no_spend"
    else:
        seg = "low_value"

    clv_records.append(
        {
            "user_id": uid,
            "session_count": scount,
            "avg_session_duration": avg_dur,
            "orders": orders,
            "total_spent": spent,
            "country": country,
            "clv_segment": seg,
        }
    )

# ---------------------------------------------------------------------------
# 2) Visualization: CLV segment distribution
# ---------------------------------------------------------------------------
print("Building Visualization 2: CLV segment distribution...")

seg_counts = Counter(r["clv_segment"] for r in clv_records)
seg_order = ["high_value", "medium_value", "low_value", "no_spend"]
seg_labels = [s for s in seg_order if s in seg_counts]
seg_values = [seg_counts[s] for s in seg_labels]

plt.figure(figsize=(8, 5))
plt.bar(seg_labels, seg_values, color="darkcyan")
plt.title("User Count by CLV Segment")
plt.xlabel("CLV Segment")
plt.ylabel("Number of Users")
plt.tight_layout()
plt.savefig("viz_clv_segments.png")
plt.close()
print("Saved: viz_clv_segments.png")

# ---------------------------------------------------------------------------
# 3) Visualization: Engagement vs Total Spending (scatter)
# ---------------------------------------------------------------------------
print("Building Visualization 3: Engagement vs Total Spending...")

# Prepare data arrays
x_sessions = []
y_spent = []
colors = []

color_map = {
    "high_value": "darkgreen",
    "medium_value": "gold",
    "low_value": "royalblue",
    "no_spend": "lightgray",
}

for r in clv_records:
    # Only plot users with some spending (optional: include 0 spend too)
    # if r["total_spent"] <= 0:
    #     continue
    x_sessions.append(r["session_count"])
    y_spent.append(r["total_spent"])
    colors.append(color_map.get(r["clv_segment"], "black"))

plt.figure(figsize=(10, 6))
plt.scatter(x_sessions, y_spent, c=colors, alpha=0.7, edgecolors="none")
plt.title("User Engagement vs Total Spending")
plt.xlabel("Session Count")
plt.ylabel("Total Spent")

# Legend: colored patches
from matplotlib.patches import Patch
legend_handles = [Patch(color=color_map[k], label=k) for k in color_map]
plt.legend(handles=legend_handles, title="CLV Segment")
plt.tight_layout()
plt.savefig("viz_engagement_vs_spending.png")
plt.close()
print("Saved: viz_engagement_vs_spending.png")

# ---------------------------------------------------------------------------
# 4) Visualization: Conversion Funnel (sessions by status)
# ---------------------------------------------------------------------------
print("Building Visualization 4: Conversion funnel...")

status_counts = Counter(s.get("conversion_status") for s in sessions)
status_order = ["browsed", "abandoned", "converted"]
status_labels = [s for s in status_order if s in status_counts]
status_values = [status_counts[s] for s in status_labels]

plt.figure(figsize=(8, 5))
plt.bar(status_labels, status_values, color="slateblue")
plt.title("Conversion Funnel: Sessions by Status")
plt.xlabel("Conversion Status")
plt.ylabel("Number of Sessions")
plt.tight_layout()
plt.savefig("viz_conversion_funnel.png")
plt.close()
print("Saved: viz_conversion_funnel.png")

print("All visualizations generated successfully.")