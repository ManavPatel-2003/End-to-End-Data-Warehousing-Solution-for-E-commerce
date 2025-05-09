import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, to_timestamp, year, current_date, floor, round, round as spark_round, current_timestamp, from_unixtime
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import to_date, rand, datediff, lit
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.types import StructType, StructField, LongType, StringType

# Initialize context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------------------- LOAD DIM DATE ------------------------------------------------
dim_date_path = 'DIM_DATE_S3_PATH'

df_dim_date = spark.read.parquet(dim_date_path)

df_dim_date = df_dim_date.withColumn("date", to_date("date"))

# ------------------------------------------------- TRANSFORMATIONS -------------------------------------------------

# --------------------------------------------------- ORDER_ITEMS ---------------------------------------------------
input_path = "S3_PATH/order_items/order_items.parquet"
df_order_items = spark.read.parquet(input_path)

# Clean and transform
df_order_items_transformed = df_order_items.withColumn(
    "order_date", to_timestamp("order_date")
).withColumn(
    "discount_percent", when(col("discount_percent").isNull(), 0).otherwise(col("discount_percent"))
).dropDuplicates(["order_id", "product_id"]
).withColumn(
    "total_amount", round(col("quantity") * col("price_each"), 2)
).withColumn(
    "net_amount", round(col("quantity") * col("price_each") * (1 - (col("discount_percent") / 100)), 2)
)

# Joining with dim_date
df_order_items_transformed = df_order_items_transformed.withColumn(
    "order_date_only", to_date("order_date")
)

df_order_items_transformed = df_order_items_transformed.join(
    df_dim_date.select("date", "date_key"),
    df_order_items_transformed["order_date_only"] == df_dim_date["date"],
    "left"
).drop("order_date_only", "date")

# Write transformed data to S3
output_path = "S3_OUTPUT_PATH/order_items/"
df_order_items_transformed.coalesce(1).write.mode("overwrite").parquet(output_path)


# ---------------------------------------------------- ORDERS ----------------------------------------------------------
df_orders = spark.read.parquet("S3_PATH/orders/orders.parquet")

from pyspark.sql.functions import col, to_timestamp, sum as _sum

# Convert order_date to proper timestamp
df_orders = df_orders.withColumn("order_date", to_timestamp(col("order_date")))

# Drop rows with null values and remove duplicates
df_orders_cleaned = df_orders.dropna().dropDuplicates()

# Calculate total and net amount per order_id from order_items
order_amounts = df_order_items_transformed.groupBy("order_id").agg(
    _sum("total_amount").alias("total_amount"),
    _sum("net_amount").alias("net_amount")
)

df_orders_transformed = df_orders_cleaned.join(order_amounts, on="order_id", how="left")

# Fill any nulls in amount columns with 0.0
df_orders_transformed = df_orders_transformed.fillna({
    "total_amount": 0.0,
    "net_amount": 0.0
})

# Join with dim_date
df_orders_transformed = df_orders_transformed.withColumn(
    "order_date_only", to_date("order_date")
)

df_orders_transformed = df_orders_transformed.join(
    df_dim_date.select("date", "date_key"),
    df_orders_transformed["order_date_only"] == df_dim_date["date"],
    "left"
).drop("order_date_only", "date")

df_orders_transformed.coalesce(1).write.mode("overwrite").parquet("S3_OUTPUT_PATH/orders/")


# --------------------------------------------------- PRODUCTS ---------------------------------------------------
df_products = spark.read.parquet("S3_PATH/products/products.parquet")

df_products_cleaned = df_products.dropna().dropDuplicates()

df_products_cleaned = df_products_cleaned.withColumn( "created_at", to_timestamp("created_at") )

df_products_transformed = df_products_cleaned.withColumn( "rating", spark_round(col("rating"), 1) )

df_products_transformed.coalesce(1).write.mode("overwrite").parquet("S3_OUTPUT_PATH/products/")


# --------------------------------------------------- RETURNS ---------------------------------------------------
df_returns = spark.read.parquet("S3_PATH/returns/returns.parquet")

# remove rows if return_id 
df_returns_filtered = df_returns.filter(~(col("return_id").isNull() | col("order_id").isNull() | col("product_id").isNull())).dropDuplicates()

df_returns_filtered = df_returns_filtered.withColumn("return_date", to_timestamp("return_date"))


refund_filled = df_returns_filtered.join(
    df_order_items_transformed.select("order_id", "product_id", "net_amount"),
    on=["order_id", "product_id"],
    how="left"
).withColumn(
    "refund_amount",
    when(col("refund_amount").isNull(), spark_round(col("net_amount") * 0.95, 2)).otherwise(col("refund_amount"))
).drop("net_amount")

refund_filled = refund_filled.withColumn("return_date_only", to_date("return_date"))

df_refund_transformed = refund_filled.join(
    df_dim_date.select("date", "date_key"),
    refund_filled["return_date_only"] == df_dim_date["date"],
    "left"
).drop("return_date_only", "date")

df_refund_transformed.coalesce(1).write.mode("overwrite").parquet("S3_OUTPUT_PATH/returns/")


# --------------------------------------------------- CUSTOMERS ---------------------------------------------------
df_customers = spark.read.parquet("S3_PATH/customers/customers.parquet")

df_customers = df_customers.filter(col("customer_id").isNotNull())

df_customers = df_customers.withColumn(
    "gender",
    when(col("gender").rlike("(?i)^m$"), "Male")
    .when(col("gender").rlike("(?i)^f$"), "Female")
    .when(col("gender").isNull(), when(rand() > 0.5, "Male").otherwise("Female"))
    .otherwise(col("gender"))
)

# Fill null date_of_birth with default date 2000-01-01
df_customers = df_customers.withColumn(
    "date_of_birth",
    when(col("date_of_birth").isNull(), F.lit("2000-01-01")).otherwise(col("date_of_birth"))
)

# Convert dates to timestamp
df_customers = df_customers.withColumn("date_of_birth", to_timestamp("date_of_birth"))
df_customers = df_customers.withColumn("signup_date", to_timestamp("signup_date"))

# Derive age and age group
df_customers = df_customers.withColumn("age", year(current_date()) - year("date_of_birth"))

df_customers = df_customers.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 28), "18-28")
    .when((col("age") >= 29) & (col("age") <= 38), "29-38")
    .when((col("age") >= 39) & (col("age") <= 48), "39-48")
    .when((col("age") >= 49) & (col("age") <= 58), "49-58")
    .when((col("age") >= 59) & (col("age") <= 70), "59-70")
    .otherwise("Other")
)

# Join with dim_date on signup_date
df_customers = df_customers.withColumn("signup_date_only", to_date("signup_date"))

df_customers_transformed = df_customers.join(
    df_dim_date.select("date", "date_key"),
    df_customers["signup_date_only"] == df_dim_date["date"],
    how="left"
).drop("signup_date_only", "date")

df_customers_transformed.coalesce(1).write.mode("overwrite").parquet("S3_OUTPUT_PATH/customers/")


# --------------------------------------------------- CAMPAIGNS ---------------------------------------------------
df_campaigns = spark.read.parquet("S3_PATH/campaigns/campaigns.parquet")

# Drop null campaign_id rows
df_campaigns = df_campaigns.filter(col("campaign_id").isNotNull())

# Remove duplicates
df_campaigns = df_campaigns.dropDuplicates()

# Convert dates to timestamp
df_campaigns = df_campaigns.withColumn("campaign_start_date", to_timestamp("campaign_start_date"))
df_campaigns = df_campaigns.withColumn("campaign_end_date", to_timestamp("campaign_end_date"))

# Join with dim_date on campaign_start_date
df_campaigns = df_campaigns.withColumn("start_date_only", to_date("campaign_start_date"))
df_campaigns_transformed = df_campaigns.join(
    df_dim_date.select("date", "date_key"),
    df_campaigns["start_date_only"] == df_dim_date["date"],
    how="left"
).drop("start_date_only", "date")

df_campaigns_transformed.coalesce(1).write.mode("overwrite").parquet("S3_OUTPUT_PATH/campaigns/")


# --------------------------------------------------- AD CLICKS ---------------------------------------------------
ad_clicks_schema = StructType([
    StructField("click_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("campaign_id", LongType(), True),
    StructField("click_time", LongType(), True),
    StructField("ad_platform", StringType(), True)
])

df_ad_clicks = spark.read.schema(ad_clicks_schema).parquet("S3_PATH/ad_clicks/ad_clicks.parquet")

# Remove rows with any nulls and drop duplicates
df_ad_clicks = df_ad_clicks.dropna().dropDuplicates()

# Convert nanoseconds to timestamp
df_ad_clicks = df_ad_clicks.withColumn(
    "click_timestamp",
    (col("click_time") / 1_000_000_000).cast("timestamp")
)

# Join with campaigns to get product_id and discount_percent
df_ad_clicks = df_ad_clicks.join(
    df_campaigns_transformed.select("campaign_id", "product_id", "discount_percent"),
    on="campaign_id",
    how="left"
)

# Join with dim_date on click date
df_ad_clicks = df_ad_clicks.withColumn("click_date", to_date("click_timestamp"))
df_ad_clicks_transformed = df_ad_clicks.join(
    df_dim_date.select("date", "date_key"),
    df_ad_clicks["click_date"] == df_dim_date["date"],
    how="left"
).drop("click_date", "date")

df_ad_clicks_transformed.coalesce(1).write.mode("overwrite").parquet("S3_OUTPUT_PATH/ad_clicks/")

# --------------------------------------------------- INVENTORY LOGS ---------------------------------------------------
df_inventory = spark.read.parquet("S3_PATH/inventory_logs/inventory_logs.parquet")
df_inventory_transformed = df_inventory.withColumn("last_updated", to_timestamp("last_updated"))
df_inventory_transformed.coalesce(1).write.mode("overwrite").parquet("S3_OUTPUT_PATH/inventory_logs/")

# --------------------------------------------------- SHIPMENTS ---------------------------------------------------
df_shipments = spark.read.parquet("S3_PATH/shipments/shipments.parquet")

# Drop rows where shipment_id or order_id is null
df_shipments = df_shipments.filter(
    col("shipment_id").isNotNull() & col("order_id").isNotNull()
)

# Drop duplicates
df_shipments = df_shipments.dropDuplicates()

# Convert dates to timestamps
df_shipments = df_shipments.withColumn("shipment_date", to_timestamp("shipment_date")) \
                           .withColumn("delivery_date", to_timestamp("delivery_date"))

# Derive shipping_duration column
df_shipments_transformed = df_shipments.withColumn(
    "shipping_duration",
    when(
        (col("shipment_status") == "Pending") | (col("delivery_date") < col("shipment_date")),
        lit(-1)
    ).otherwise(
        datediff(col("delivery_date"), col("shipment_date"))
    )
)

# Determine if delivery is delayed
df_shipments_transformed = df_shipments_transformed.withColumn( 
    "is_delayed_delivery", 
    when(col("shipping_duration") == -1, 0) 
    .when((col("shipment_status") == "Delivered") & (col("shipping_duration") > 4), 1) 
    .otherwise(0) 
)

# Write cleaned shipments data to S3
df_shipments_transformed.coalesce(1).write.mode("overwrite").parquet("S3_OUTPUT_PATH/shipments/")

# --------------------------------------------------- REVIEWS ---------------------------------------------------
reviews_schema = StructType([
    StructField("review_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("review_date", LongType(), True),
    StructField("rating", LongType(), True),
    StructField("review_text", StringType(), True),
    StructField("sentiment", StringType(), True)
])

df_reviews = spark.read.schema(reviews_schema).parquet("S3_PATH/reviews/reviews.parquet")

# Convert review_date from nanoseconds to timestamp
df_reviews_transformed = df_reviews.withColumn(
    "review_date",
    from_unixtime((col("review_date") / 1_000_000_000).cast("long")).cast("timestamp")
)

# Write cleaned reviews data
df_reviews_transformed.coalesce(1).write.mode("overwrite").parquet("S3_OUTPUT_PATH/reviews/")

# --------------------------------------------------- WEB SESSIONS ---------------------------------------------------
web_sessions_schema = StructType([
    StructField("session_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("session_start", LongType(), True),
    StructField("session_end", LongType(), True),
    StructField("device_type", StringType(), True),
    StructField("browser", StringType(), True)
])

df_web_sessions = spark.read.schema(web_sessions_schema).parquet("S3_PATH/web_sessions/web_sessions.parquet")

# Convert session_start and session_end from nanoseconds to timestamp
df_web_sessions = df_web_sessions.withColumn(
    "session_start",
    from_unixtime((col("session_start") / 1_000_000_000).cast("long")).cast("timestamp")
).withColumn(
    "session_end",
    from_unixtime((col("session_end") / 1_000_000_000).cast("long")).cast("timestamp")
)

# Join with dim_date using session_start date
df_web_sessions_transformed = df_web_sessions.withColumn(
    "session_start_date", to_date("session_start")
)

df_web_sessions_transformed = df_web_sessions_transformed.join(
    df_dim_date.select("date", "date_key"),
    df_web_sessions_transformed["session_start_date"] == df_dim_date["date"],
    "left"
).drop("session_start_date", "date")

# Write cleaned data to S3
df_web_sessions_transformed.coalesce(1).write.mode("overwrite").parquet("S3_OUTPUT_PATH/web_sessions/")

# --------------------------------------------------- CART ACTIVITY ---------------------------------------------------
cart_activity_schema = StructType([
    StructField("activity_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("activity_type", StringType(), True),
    StructField("activity_time", LongType(), True)
])
df_cart_activity = spark.read.schema(cart_activity_schema).parquet("S3_PATH/cart_activity/cart_activity.parquet")

# Convert activity_time from nanoseconds to timestamp
df_cart_activity = df_cart_activity.withColumn(
    "activity_time",
    from_unixtime((col("activity_time") / 1_000_000_000).cast("long")).cast("timestamp")
)

# Join with dim_date on activity_time
df_cart_activity_transformed = df_cart_activity.withColumn(
    "activity_date", to_date("activity_time")
)

df_cart_activity_transformed = df_cart_activity_transformed.join(
    df_dim_date.select("date", "date_key"),
    df_cart_activity_transformed["activity_date"] == df_dim_date["date"],
    "left"
).drop("activity_date", "date")

df_cart_activity_transformed.coalesce(1).write.mode("overwrite").parquet("S3_OUTPUT_PATH/cart_activity/")

job.commit()