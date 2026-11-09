from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# ============================================================
# 1. CONFIGURATION
# ============================================================

CATALOG = dbutils.widgets.text("CATALOG", "hop")
CATALOG = dbutils.widgets.get("CATALOG")

BRONZE_TABLE = f"{CATALOG}.bronze.order_events"
EVENTS_DEDUP_TABLE = f"{CATALOG}.silver.order_events_dedup"
ORDERS_TABLE = f"{CATALOG}.silver.orders"
QUARANTINE_TABLE = f"{CATALOG}.silver.orders_quarantine"

# Reference tables for Q-A (unknown reference) checks
CUSTOMERS_TABLE = f"{CATALOG}.silver.customers"
RIDERS_TABLE = f"{CATALOG}.silver.riders"
ZONES_TABLE = f"{CATALOG}.silver.zones"
RESTAURANTS_TABLE = f"{CATALOG}.silver.dim_restaurant_scd2"

VALID_DELIVERY_CLASSES = ["instant", "standard"]
INSTANT_PROMISE = 10
STANDARD_PROMISES = [30, 35, 40, 45]


def standardize_events(df: DataFrame) -> DataFrame:
    """
    Clean Bronze event columns into proper Silver types.
    """

    df = (
        df
        .withColumn("event_id", F.trim(F.col("event_id").cast("STRING")))
        .withColumn("order_id", F.trim(F.col("order_id").cast("STRING")))
        .withColumn("event_type", F.trim(F.col("event_type").cast("STRING")))
        .withColumn("customer_id", F.trim(F.col("customer_id").cast("STRING")))
        .withColumn("restaurant_id", F.trim(F.col("restaurant_id").cast("STRING")))
        .withColumn("zone_id", F.trim(F.col("zone_id").cast("STRING")))
        .withColumn("rider_id", F.trim(F.col("rider_id").cast("STRING")))
        .withColumn("delivery_class", F.lower(F.trim(F.col("delivery_class").cast("STRING"))))
        .withColumn("cancelled_by", F.trim(F.col("cancelled_by").cast("STRING")))

        # event_ts is ISO format with 'T' separator in Bronze
        .withColumn("event_ts", F.expr("try_to_timestamp(event_ts)"))

        # Safe numeric casts
        .withColumn("promised_minutes", F.expr("try_cast(promised_minutes AS INT)"))
        .withColumn("order_value", F.expr("try_cast(order_value AS DOUBLE)"))
    )

    # platform is not present in the current Bronze table;
    # keep it NULL until the source provides it
    if "platform" not in df.columns:
        df = df.withColumn("platform", F.lit(None).cast("STRING"))

    return df


def dedup_events(df: DataFrame) -> DataFrame:
    """
    One row per event_id: keep the earliest event_ts for each event_id.
    """

    w = Window.partitionBy("event_id").orderBy(F.col("event_ts").asc())

    df = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return df


# ============================================================
# 3. REBUILD ORDERS FROM EVENTS
# ============================================================

def build_orders(df: DataFrame) -> DataFrame:
    """
    Combine all events of an order into one row.

    - Each event time = earliest event_ts of that event type.
    - customer / restaurant / zone / delivery_class / promise /
      value / platform come from the placed event.
    - rider comes from the picked_up or delivered event.
    - cancelled_by comes from the cancelled event.
    """

    def from_event(event_type: str, col: str):
        return F.max(F.when(F.col("event_type") == event_type, F.col(col)))

    def earliest_ts(event_type: str):
        return F.min(F.when(F.col("event_type") == event_type, F.col("event_ts")))

    orders_df = (
        df
        .groupBy("order_id")
        .agg(
            earliest_ts("placed").alias("placed_ts"),
            earliest_ts("accepted").alias("accepted_ts"),
            earliest_ts("picked_up").alias("picked_up_ts"),
            earliest_ts("delivered").alias("delivered_ts"),
            earliest_ts("cancelled").alias("cancelled_ts"),

            from_event("placed", "customer_id").alias("customer_id"),
            from_event("placed", "restaurant_id").alias("restaurant_id"),
            from_event("placed", "zone_id").alias("zone_id"),
            from_event("placed", "delivery_class").alias("delivery_class"),
            from_event("placed", "promised_minutes").alias("promised_minutes"),
            from_event("placed", "order_value").alias("order_value"),
            from_event("placed", "platform").alias("platform"),

            F.coalesce(
                from_event("picked_up", "rider_id"),
                from_event("delivered", "rider_id")
            ).alias("rider_id"),

            from_event("cancelled", "cancelled_by").alias("cancelled_by")
        )
        # status: delivered when there is a delivered event,
        # otherwise cancelled when there is a cancelled event
        .withColumn(
            "status",
            F.when(F.col("delivered_ts").isNotNull(), F.lit("delivered"))
            .otherwise(F.lit("cancelled"))
        )
    )

    return orders_df


# ============================================================
# 4. VALIDATE ORDERS (QUARANTINE RULES)
# ============================================================

def validate_orders(orders_df: DataFrame):
    """
    Apply Q-A / Q-B / Q-C / Q-D rules.
    An order breaking any rule goes to quarantine with all
    broken rule ids in failed_rules.
    """

    # --- Reference sets (distinct ids from Silver reference tables)
    customers_ref = (
        spark.table(CUSTOMERS_TABLE)
        .select(F.col("customer_id").alias("ref_customer_id"))
        .distinct()
    )
    restaurants_ref = (
        spark.table(RESTAURANTS_TABLE)
        .select(F.col("restaurant_id").alias("ref_restaurant_id"))
        .distinct()
    )
    zones_ref = (
        spark.table(ZONES_TABLE)
        .select(F.col("zone_id").alias("ref_zone_id"))
        .distinct()
    )
    riders_ref = (
        spark.table(RIDERS_TABLE)
        .select(F.col("rider_id").alias("ref_rider_id"))
        .distinct()
    )

    df = (
        orders_df
        .join(customers_ref, orders_df["customer_id"] == customers_ref["ref_customer_id"], "left")
        .join(restaurants_ref, orders_df["restaurant_id"] == restaurants_ref["ref_restaurant_id"], "left")
        .join(zones_ref, orders_df["zone_id"] == zones_ref["ref_zone_id"], "left")
        .join(riders_ref, orders_df["rider_id"] == riders_ref["ref_rider_id"], "left")
    )

    # Q-A: unknown reference (rider only when a rider is named)
    q_a = (
        F.col("ref_customer_id").isNull()
        | F.col("ref_restaurant_id").isNull()
        | F.col("ref_zone_id").isNull()
        | (F.col("rider_id").isNotNull() & F.col("ref_rider_id").isNull())
    )

    # Q-B: not exactly one of delivered or cancelled
    q_b = (
        (F.col("delivered_ts").isNotNull() & F.col("cancelled_ts").isNotNull())
        | (F.col("delivered_ts").isNull() & F.col("cancelled_ts").isNull())
    )

    # Q-C: clock runs backwards (only when both timestamps exist)
    q_c = (
        F.coalesce(F.col("accepted_ts") < F.col("placed_ts"), F.lit(False))
        | F.coalesce(F.col("picked_up_ts") < F.col("accepted_ts"), F.lit(False))
        | F.coalesce(F.col("delivered_ts") < F.col("picked_up_ts"), F.lit(False))
    )

    # Q-D: bad values
    q_d = (
        F.col("order_value").isNull()
        | (F.col("order_value") <= 0)
        | ~F.coalesce(F.col("delivery_class").isin(VALID_DELIVERY_CLASSES), F.lit(False))
        | (
            F.coalesce(F.col("delivery_class") == "instant", F.lit(False))
            & (
                F.col("promised_minutes").isNull()
                | F.coalesce(F.col("promised_minutes") != INSTANT_PROMISE, F.lit(True))
            )
        )
        | (
            F.coalesce(F.col("delivery_class") == "standard", F.lit(False))
            & (
                F.col("promised_minutes").isNull()
                | ~F.coalesce(F.col("promised_minutes").isin(STANDARD_PROMISES), F.lit(False))
            )
        )
    )

    df = df.withColumn(
        "failed_rules",
        F.array_compact(
            F.array(
                F.when(q_a, F.lit("Q-A")),
                F.when(q_b, F.lit("Q-B")),
                F.when(q_c, F.lit("Q-C")),
                F.when(q_d, F.lit("Q-D"))
            )
        )
    )

    quarantine_df = (
        df
        .filter(F.size("failed_rules") > 0)
        .select("order_id", "failed_rules")
    )

    valid_df = (
        df
        .filter(F.size("failed_rules") == 0)
        .select(
            "order_id",
            "status",
            "customer_id",
            "restaurant_id",
            "zone_id",
            "delivery_class",
            "promised_minutes",
            "order_value",
            "platform",
            "rider_id",
            "cancelled_by",
            "placed_ts",
            "accepted_ts",
            "picked_up_ts",
            "delivered_ts",
            "cancelled_ts"
        )
    )

    return valid_df, quarantine_df

# ============================================================
# 5. WRITE TABLES
# ============================================================

def write_table(df: DataFrame, table_name: str):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

# ============================================================
# 6. MAIN
# ============================================================

def main():

    # Step 1: Read + standardize + dedup Bronze events
    bronze_df = spark.table(BRONZE_TABLE)
    print("Bronze events:", bronze_df.count())

    events_df = dedup_events(standardize_events(bronze_df))
    print("Deduped events (one per event_id):", events_df.count())

    # Step 2: Write deduped events (columns of the bronze table)
    events_silver_df = events_df.select(
        "customer_id", "delivery_class", "event_id", "event_ts",
        "event_type", "order_id", "order_value", "promised_minutes",
        "restaurant_id", "zone_id", "rider_id", "cancelled_by",
        "_rescued_data", "_ingested_at", "_source_file"
    )
    write_table(events_silver_df, EVENTS_DEDUP_TABLE)

    # Step 3: Rebuild one row per order
    orders_df = build_orders(events_df)
    print("Rebuilt orders:", orders_df.count())

    # Step 4: Validate and split into orders / quarantine
    valid_df, quarantine_df = validate_orders(orders_df)

    print("Clean orders:", valid_df.count())
    print("Quarantined orders:", quarantine_df.count())

    # Step 5: Write final tables
    write_table(valid_df, ORDERS_TABLE)
    write_table(quarantine_df, QUARANTINE_TABLE)

    print("Order events processing completed successfully.")


# ============================================================
# 7. EXECUTION
# ============================================================

if __name__ == "__main__":
    main()