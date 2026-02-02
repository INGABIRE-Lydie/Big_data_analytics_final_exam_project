from pyspark.sql import SparkSession, functions as F, types as T
# Spark session (MongoDB Spark Connector)
spark = (
    SparkSession.builder
    .appName("EcommerceAnalytics_Integrated")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
# MongoDB connection URIs (change host/port/db if needed)
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/e_commerce")
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/e_commerce")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

#  read MongoDB collection as DataFrame

def read_mongo(collection: str):
    return (
        spark.read.format("mongodb")
        .option("collection", collection)
        .load()
    )


# 1) READ FROM MONGODB

print("\n=== Reading from MongoDB ===")
users_df        = read_mongo("users")
products_df     = read_mongo("products")
categories_df   = read_mongo("categories")
transactions_df = read_mongo("transactions")

print("MongoDB counts:")
print("  users       :", users_df.count())
print("  products    :", products_df.count())
print("  categories  :", categories_df.count())
print("  transactions:", transactions_df.count())

# 2) BASIC CLEANING / NORMALIZATION
# Users: normalize dates
users_df = (
    users_df
    .withColumn("registration_ts", F.to_timestamp("registration_date"))
    .withColumn("last_active_ts", F.to_timestamp("last_active"))
)

# Products: numeric types
products_df = (
    products_df
    .withColumn("base_price", F.col("base_price").cast(T.DoubleType()))
    .withColumn("current_stock", F.col("current_stock").cast(T.IntegerType()))
)

# Transactions: numeric types + normalized timestamp
transactions_df = (
    transactions_df
    .withColumn("timestamp_ts", F.to_timestamp("timestamp"))
    .withColumn("subtotal", F.col("subtotal").cast(T.DoubleType()))
    .withColumn("discount", F.col("discount").cast(T.DoubleType()))
    .withColumn("total", F.col("total").cast(T.DoubleType()))
)


# 3) READ SESSIONS FROM HBASE (via Thrift) OR FALLBACK TO sessions.json

print("\n=== Reading sessions from HBase (Thrift) with fallback to sessions.json ===")

def try_read_sessions_from_hbase(
    table_name="ecom:user_sessions",
    thrift_host="localhost",
    thrift_port=9090,
    user_prefix=None,         
    limit=50000
):
    """
    Attempts to read sessions from HBase using HappyBase (Thrift).
    Returns a Spark DataFrame if successful, else None.
    """
    try:
        import happybase
    except Exception as e:
        print("WARNING: happybase not installed. Falling back to sessions.json.")
        return None

    try:
        conn = happybase.Connection(host=thrift_host, port=thrift_port, timeout=5000)
        conn.open()
        table = conn.table(table_name)

        rows = []
        count = 0

        scan_kwargs = {}
        if user_prefix:
            scan_kwargs["row_prefix"] = user_prefix

        for rk, data in table.scan(**scan_kwargs):
            row = {"row_key": rk.decode("utf-8")}
            for k, v in data.items():
                # k like b's:start_time', v like b'2026-01-17T...'
                row[k.decode("utf-8")] = v.decode("utf-8")
            rows.append(row)
            count += 1
            if count >= limit:
                break

        conn.close()

        if not rows:
            print("WARNING: HBase scan returned 0 rows. Falling back to sessions.json.")
            return None

        df = spark.createDataFrame(rows)

        # Normalize key fields (adjust to actual qualifiers)
        # pv:page_count, pv:product_detail_views
        df = (
            df
            .withColumn("user_id", F.split(F.col("row_key"), "#").getItem(0))
            .withColumn("duration_seconds", F.col("s:duration_seconds").cast("int"))
            .withColumn("page_count", F.col("pv:page_count").cast("int"))
            .withColumn("product_detail_views", F.col("pv:product_detail_views").cast("int"))
            .withColumnRenamed("s:start_time", "start_time")
            .withColumnRenamed("s:end_time", "end_time")
            .withColumnRenamed("s:conversion_status", "conversion_status")
        )

        print(f"SUCCESS: Loaded {df.count()} sessions from HBase (subset).")
        return df

    except Exception as e:
        print(f"WARNING: Could not read from HBase Thrift ({thrift_host}:{thrift_port}).")
        print("Reason:", str(e))
        print("Falling back to sessions.json.")
        return None


# Try HBase first
sessions_df = try_read_sessions_from_hbase(limit=5000)


# Fallback: local JSON sessions
if sessions_df is None:
    sessions_df = (
        spark.read.json("sessions.json")
        .withColumn("duration_seconds", F.col("duration_seconds").cast(T.IntegerType()))
    )
    print("Loaded sessions from sessions.json:", sessions_df.count())


# 4) BATCH ANALYTIC: Product co-occurrence (bought together)

trans_products_df = transactions_df.select(
    "transaction_id",
    F.expr("transform(items, x -> x.product_id)").alias("products")
)

trans_products_df = (
    trans_products_df
    .select("transaction_id", F.array_distinct(F.expr("filter(products, x -> x is not null)")).alias("products"))
    .where(F.size("products") >= 2)
)

exploded_a = trans_products_df.select("transaction_id", F.explode("products").alias("prod_a"))
exploded_b = trans_products_df.select("transaction_id", F.explode("products").alias("prod_b"))

pairs_df = (
    exploded_a.join(exploded_b, on="transaction_id")
    .where(F.col("prod_a") < F.col("prod_b"))
)

cooc_df = (
    pairs_df.groupBy("prod_a", "prod_b")
    .agg(F.count("*").alias("cooc_count"))
    .orderBy(F.desc("cooc_count"))
)

print("Top 10 product pairs bought together (by product_id):")
cooc_df.show(10, truncate=False)

# Attach product names
prod_names = products_df.select(F.col("product_id").alias("pid"), "name")

cooc_named_df = (
    cooc_df
    .join(prod_names, cooc_df.prod_a == prod_names.pid, "left")
    .withColumnRenamed("name", "prod_a_name")
    .drop("pid")
    .join(prod_names, cooc_df.prod_b == prod_names.pid, "left")
    .withColumnRenamed("name", "prod_b_name")
    .drop("pid")
)

print("Top 10 product pairs with names:")
cooc_named_df.select("prod_a", "prod_a_name", "prod_b", "prod_b_name", "cooc_count").show(10, truncate=False)


# 6) COHORT ANALYSIS (registration month -> spending over subsequent months)
# -----------------------------------------------------------------------------
print("\n=== Cohort Analysis: Registration Month vs Subsequent Spending ===")

users_cohort = users_df.select(
    "user_id",
    F.date_trunc("month", F.col("registration_ts")).alias("cohort_month")
)

tx_month = transactions_df.select(
    "user_id",
    F.date_trunc("month", F.col("timestamp_ts")).alias("order_month"),
    F.col("total").cast("double").alias("total")
)

cohort_df = (
    tx_month.join(users_cohort, on="user_id", how="inner")
    .withColumn("months_since_cohort", F.months_between(F.col("order_month"), F.col("cohort_month")).cast("int"))
    .groupBy("cohort_month", "months_since_cohort")
    .agg(
        F.count("*").alias("num_orders"),
        F.round(F.sum("total"), 2).alias("total_revenue"),
        F.round(F.avg("total"), 2).alias("avg_order_value")
    )
    .orderBy("cohort_month", "months_since_cohort")
)

cohort_df.show(50, truncate=False)
# 5) SPARK SQL ANALYTICS

users_df.createOrReplaceTempView("users")
products_df.createOrReplaceTempView("products")
categories_df.createOrReplaceTempView("categories")
transactions_df.createOrReplaceTempView("transactions")
sessions_df.createOrReplaceTempView("sessions")

print("\n=== Spark SQL: Revenue by category ===")
revenue_by_cat_sql = """
SELECT
    p.category_id,
    SUM(i.subtotal) AS total_revenue,
    SUM(i.quantity) AS total_quantity,
    COUNT(*)        AS line_items
FROM (
    SELECT explode(items) AS i
    FROM transactions
) t
JOIN products p
    ON t.i.product_id = p.product_id
GROUP BY
    p.category_id
ORDER BY
    total_revenue DESC
LIMIT 10
"""
spark.sql(revenue_by_cat_sql).show(truncate=False)

print("\n=== Spark SQL: Basic user spending summary ===")
user_spend_sql = """
SELECT
    user_id,
    COUNT(*)     AS orders,
    SUM(total)   AS total_spent,
    AVG(total)   AS avg_order_value
FROM
    transactions
GROUP BY
    user_id
ORDER BY
    total_spent DESC
LIMIT 10
"""
spark.sql(user_spend_sql).show(truncate=False)

print("\n=== Spark SQL: Top products by revenue ===")
top_products_sql = """
SELECT
    i.product_id,
    p.name,
    SUM(i.subtotal) AS total_revenue,
    SUM(i.quantity) AS total_quantity
FROM (
    SELECT explode(items) AS i
    FROM transactions
) t
JOIN products p
    ON t.i.product_id = p.product_id
GROUP BY
    i.product_id, p.name
ORDER BY
    total_revenue DESC
LIMIT 10
"""
spark.sql(top_products_sql).show(truncate=False)



 #6) INTEGRATED ANALYTICS: CLV (spend) vs engagement (sessions)

print("\n=== Integrated CLV (spending) vs engagement (sessions) ===")

clv_engagement_sql = """
WITH session_stats AS (
    SELECT
        user_id,
        COUNT(*) AS session_count,
        AVG(COALESCE(duration_seconds, 0)) AS avg_session_duration
    FROM sessions
    GROUP BY user_id
),
spend_stats AS (
    SELECT
        user_id,
        COUNT(*) AS orders,
        SUM(total) AS total_spent,
        AVG(total) AS avg_order_value
    FROM transactions
    GROUP BY user_id
),
clv_segmented AS (
    SELECT
        s.user_id,
        s.session_count,
        s.avg_session_duration,
        sp.orders,
        sp.total_spent,
        sp.avg_order_value,
        CASE
            WHEN sp.total_spent >= 500 THEN 'high_value'
            WHEN sp.total_spent >= 200 THEN 'medium_value'
            WHEN sp.total_spent IS NULL THEN 'no_spend'
            ELSE 'low_value'
        END AS clv_segment
    FROM session_stats s
    LEFT JOIN spend_stats sp
        ON s.user_id = sp.user_id
)
SELECT
    u.user_id,
    u.geo_data.country AS country,
    u.registration_date,
    c.session_count,
    ROUND(c.avg_session_duration, 2) AS avg_session_duration,
    c.orders,
    ROUND(c.total_spent, 2) AS total_spent,
    ROUND(c.avg_order_value, 2) AS avg_order_value,
    c.clv_segment
FROM clv_segmented c
JOIN users u
    ON u.user_id = c.user_id
ORDER BY c.total_spent DESC NULLS LAST
LIMIT 5

"""

clv_engagement_df = spark.sql(clv_engagement_sql)
clv_engagement_df.show(20, truncate=False)

spark.stop()
print("\nDone.")
