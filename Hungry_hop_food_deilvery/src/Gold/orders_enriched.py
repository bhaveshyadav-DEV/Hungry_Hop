from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# ============================================================
# 1. CONFIGURATION
# ============================================================

CATALOG = dbutils.widgets.text("CATALOG", "hop")
CATALOG = dbutils.widgets.get("CATALOG")

ORDERS_TABLE = f"{CATALOG}.silver.orders"
CUSTOMERS_TABLE = f"{CATALOG}.silver.customers"
ZONES_TABLE = f"{CATALOG}.silver.zones"
RESTAURANTS_SCD2_TABLE = f"{CATALOG}.silver.dim_restaurant_scd2"
GOLD_TABLE = f"{CATALOG}.gold.orders_enriched"


# ============================================================
# 2. ENRICH ORDERS WITH REFERENCE DATA
# ============================================================

def enrich_orders(orders_df: DataFrame) -> DataFrame:
    """
    One row per clean order, enriched with:

    - customer city / tier from Silver customers
    - zone city / type from Silver zones
    - restaurant name / zone_id / rating from the
      dim_restaurant_scd2 version in force at placed_ts
      (point-in-time SCD2 join)
    """

    # --- Reference frames (renamed to avoid ambiguity)
    customers_ref = (
        spark.table(CUSTOMERS_TABLE)
        .select(
            F.col("customer_id").alias("c_customer_id"),
            F.col("city").alias("customer_city"),
            F.col("tier").alias("customer_tier")
        )
    )

    zones_ref = (
        spark.table(ZONES_TABLE)
        .select(
            F.col("zone_id").alias("z_zone_id"),
            F.col("city").alias("zone_city"),
            F.col("zone_type").alias("zone_type")
        )
    )

    restaurants_scd2 = (
        spark.table(RESTAURANTS_SCD2_TABLE)
        .select(
            F.col("restaurant_id").alias("r_restaurant_id"),
            F.col("name").alias("restaurant_name"),
            F.col("zone_id").alias("restaurant_zone_id_at_order"),
            F.col("rating").alias("restaurant_rating_at_order"),
            F.col("START_AT").alias("r_start_at"),
            F.col("END_AT").alias("r_end_at")
        )
    )

    # --- Customer + zone enrichment
    df = (
        orders_df
        .join(customers_ref, orders_df["customer_id"] == customers_ref["c_customer_id"], "left")
        .join(zones_ref, orders_df["zone_id"] == zones_ref["z_zone_id"], "left")
    )

    # --- Restaurant enrichment: SCD2 version in force at placed_ts
    # [START_AT, END_AT) interval; END_AT NULL = current version
    df = df.join(
        restaurants_scd2,
        (orders_df["restaurant_id"] == restaurants_scd2["r_restaurant_id"])
        & (orders_df["placed_ts"] >= restaurants_scd2["r_start_at"])
        & (
            restaurants_scd2["r_end_at"].isNull()
            | (orders_df["placed_ts"] < restaurants_scd2["r_end_at"])
        ),
        "left"
    )

    return df


# ============================================================
# 3. DERIVED GOLD COLUMNS
# ============================================================

def with_derived_columns(df: DataFrame) -> DataFrame:
    """
    Add timing / promise columns (all in minutes, DOUBLE).
    """

    df = (
        df
        .withColumn("placed_date", F.to_date("placed_ts"))
        .withColumn("placed_to_accepted_minutes", F.expr("(unix_timestamp(accepted_ts) - unix_timestamp(placed_ts)) / 60.0").cast("DOUBLE"))
        .withColumn("prep_minutes", F.expr("(unix_timestamp(picked_up_ts) - unix_timestamp(accepted_ts)) / 60.0").cast("DOUBLE"))
        .withColumn("actual_minutes", F.expr("(unix_timestamp(delivered_ts) - unix_timestamp(accepted_ts)) / 60.0").cast("DOUBLE"))
        .withColumn("minutes_past_promise", F.expr("actual_minutes - promised_minutes").cast("DOUBLE"))
    )

    return df

# ============================================================
# 4. PREPARE GOLD OUTPUT
# ============================================================

def prepare_gold_output(df: DataFrame) -> DataFrame:
    """
    Select exactly the required Gold columns, in order.
    """

    gold_df = df.select(
        "order_id",
        "status",
        "delivery_class",
        "promised_minutes",
        "order_value",
        "customer_id",
        "customer_city",
        "customer_tier",
        F.col("zone_id").alias("delivery_zone_id"),
        "zone_city",
        "zone_type",
        "restaurant_id",
        "restaurant_name",
        "restaurant_zone_id_at_order",
        "restaurant_rating_at_order",
        "rider_id",
        "platform",
        "placed_ts",
        "accepted_ts",
        "picked_up_ts",
        "delivered_ts",
        "cancelled_ts",
        "cancelled_by",
        "placed_date",
        "placed_to_accepted_minutes",
        "prep_minutes",
        "actual_minutes",
        "minutes_past_promise"
    )

    return gold_df

def main():
    orders_df = spark.table(ORDERS_TABLE)
    print("Clean Silver orders:", orders_df.count())

    final_df = prepare_gold_output(with_derived_columns(enrich_orders(orders_df)))

    (
        final_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_TABLE)
    )

    print("Gold orders_enriched rows written:", final_df.count())
    print("Gold processing completed successfully.")


if __name__ == "__main__":
    main()