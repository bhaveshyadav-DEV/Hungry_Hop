# ============================================================
# RESTAURANT CDC - SCD TYPE 2 IMPLEMENTATION
# Bronze -> Quarantine + Silver SCD2
# ============================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. CONFIGURATION
# ============================================================

CATALOG = "hop"

BRONZE_TABLE = f"{CATALOG}.bronze.restaurants_cdc"

# CHANGE THIS IF YOUR ZONE MASTER TABLE NAME IS DIFFERENT
ZONE_TABLE = f"{CATALOG}.silver.zones"

SILVER_TABLE = f"{CATALOG}.silver.dim_restaurant_scd2"

QUARANTINE_TABLE = f"{CATALOG}.silver.quarantine_restaurants"


# ============================================================
# 2. READ BRONZE DATA
# ============================================================

def read_bronze_data() -> DataFrame:
    """
    Read complete CDC history from Bronze.
    Preserve the original record before standardization.
    """

    df = spark.table(BRONZE_TABLE)

    df = df.withColumn(
        "raw_record",
        F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    )

    return df


# ============================================================
# 3. STANDARDIZE DATA
# ============================================================

def standardize_data(df: DataFrame) -> DataFrame:
    """
    Standardize string columns and cast data types.
    """

    df = (
        df
        .withColumn(
            "restaurant_id",
            F.trim(F.col("restaurant_id").cast("string"))
        )
        .withColumn(
            "name",
            F.trim(F.col("name").cast("string"))
        )
        .withColumn(
            "cuisine",
            F.trim(F.col("cuisine").cast("string"))
        )
        .withColumn(
            "zone_id",
            F.trim(F.col("zone_id").cast("string"))
        )
        .withColumn(
            "op",
            F.upper(F.trim(F.col("op").cast("string")))
        )
        .withColumn(
            "rating_raw",
            F.col("rating").cast("string")
        )
        .withColumn(
            "rating",
            F.col("rating").cast("double")
        )
        .withColumn(
            "is_active_raw",
            F.lower(F.trim(F.col("is_active").cast("string")))
        )
        .withColumn(
            "is_active",
            F.when(
                F.col("is_active_raw").isin("true", "1", "yes", "y"),
                F.lit(True)
            )
            .when(
                F.col("is_active_raw").isin("false", "0", "no", "n"),
                F.lit(False)
            )
            .otherwise(F.lit(None).cast("boolean"))
        )
        .withColumn(
            "op_ts_raw",
            F.col("op_ts").cast("string")
        )
        .withColumn(
            "op_ts",
            F.coalesce(
                F.expr("try_to_timestamp(op_ts_raw, 'yyyy-MM-dd HH:mm:ss')"),
                F.expr("try_to_timestamp(op_ts_raw, \"yyyy-MM-dd'T'HH:mm:ss\")"),
                F.expr("try_to_timestamp(op_ts_raw)")
            )
        )
    )

    return df


# ============================================================
# 4. VALIDATE ZONE MASTER
# ============================================================

def read_zone_master() -> DataFrame:
    """
    Read valid zone IDs from the zone master table.
    """

    zones_df = (
        spark.table(ZONE_TABLE)
        .select(
            F.trim(F.col("zone_id").cast("string")).alias("valid_zone_id")
        )
        .filter(
            F.col("valid_zone_id").isNotNull()
            & (F.col("valid_zone_id") != "")
        )
        .dropDuplicates(["valid_zone_id"])
    )

    return zones_df


# ============================================================
# 5. QUARANTINE INVALID RECORDS
# ============================================================

def quarantine_invalid_records(
    df: DataFrame,
    zones_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Separate valid and invalid CDC records.

    Invalid records are quarantined with a rejection reason.
    DELETE events do not require zone validation.
    """

    df = df.join(
        F.broadcast(zones_df),
        df.zone_id == zones_df.valid_zone_id,
        "left"
    )

    df = df.withColumn(
        "reject_reason",

        F.when(
            F.col("restaurant_id").isNull()
            | (F.col("restaurant_id") == ""),
            F.lit("null_restaurant_id")
        )

        .when(
            F.col("op_ts").isNull(),
            F.lit("invalid_op_ts")
        )

        .when(
            ~F.col("op").isin("INSERT", "UPDATE", "DELETE")
            | F.col("op").isNull(),
            F.lit("invalid_operation")
        )

        .when(
            (F.col("op") != "DELETE")
            & F.col("valid_zone_id").isNull(),
            F.lit("invalid_zone")
        )

        .when(
            (F.col("rating_raw").isNotNull())
            & F.col("rating").isNull(),
            F.lit("invalid_rating")
        )

        .when(
            (F.col("is_active_raw").isNotNull())
            & F.col("is_active").isNull()
            & (F.col("op") != "DELETE"),
            F.lit("invalid_is_active")
        )

        .otherwise(F.lit(None).cast("string"))
    )

    # --------------------------------------------------------
    # Detect duplicate events for the same restaurant/timestamp
    # --------------------------------------------------------

    duplicate_window = Window.partitionBy(
        "restaurant_id",
        "op_ts"
    )

    df = df.withColumn(
        "event_count",
        F.count("*").over(duplicate_window)
    )

    df = df.withColumn(
        "reject_reason",

        F.when(
            F.col("reject_reason").isNotNull(),
            F.col("reject_reason")
        )

        .when(
            F.col("event_count") > 1,
            F.lit("duplicate_event_timestamp")
        )

        .otherwise(F.lit(None).cast("string"))
    )

    # --------------------------------------------------------
    # Split valid and invalid records
    # --------------------------------------------------------

    invalid_df = (
        df
        .filter(F.col("reject_reason").isNotNull())
        .drop("valid_zone_id", "event_count")
    )

    valid_df = (
        df
        .filter(F.col("reject_reason").isNull())
        .drop("valid_zone_id", "event_count", "reject_reason")
    )

    return valid_df, invalid_df


# ============================================================
# 6. WRITE QUARANTINE TABLE
# ============================================================

def write_quarantine(invalid_df: DataFrame) -> None:
    """
    Overwrite quarantine table with the complete invalid set.

    This is a full-history rebuild, so overwrite avoids
    inserting the same rejected events again on every run.
    """

    quarantine_df = invalid_df.select(
        "restaurant_id",
        "name",
        "cuisine",
        "zone_id",
        "rating",
        "is_active",
        "op",
        "op_ts",
        "reject_reason",
        "raw_record"
    )

    (
        quarantine_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(QUARANTINE_TABLE)
    )


# ============================================================
# 7. IDENTIFY SCD2 CHANGE EVENTS
# ============================================================

def identify_scd2_changes(valid_df: DataFrame) -> DataFrame:
    """
    Create a new version when any tracked attribute changes.

    Tracked attributes:
        - rating
        - zone_id
        - is_active

    DELETE events are treated as version-closing boundaries.
    """

    event_window = (
        Window
        .partitionBy("restaurant_id")
        .orderBy("op_ts")
    )

    df = (
        valid_df
        .withColumn("previous_op", F.lag("op").over(event_window))
        .withColumn("previous_rating", F.lag("rating").over(event_window))
        .withColumn("previous_zone_id", F.lag("zone_id").over(event_window))
        .withColumn("previous_is_active", F.lag("is_active").over(event_window))
    )

    # --------------------------------------------------------
    # Detect changes using null-safe comparison
    # --------------------------------------------------------

    tracked_attribute_changed = (
        ~F.col("rating").eqNullSafe(F.col("previous_rating"))
        |
        ~F.col("zone_id").eqNullSafe(F.col("previous_zone_id"))
        |
        ~F.col("is_active").eqNullSafe(F.col("previous_is_active"))
    )

    # --------------------------------------------------------
    # New version conditions
    # --------------------------------------------------------

    is_first_event = F.col("previous_op").isNull()

    after_delete = F.col("previous_op") == "DELETE"

    is_new_version = (
        is_first_event
        | after_delete
        | tracked_attribute_changed
    )

    df = df.withColumn(
        "is_new_version",

        F.when(
            F.col("op").isin("INSERT", "UPDATE"),
            is_new_version
        ).otherwise(F.lit(False))
    )

    # --------------------------------------------------------
    # DELETE boundary condition
    # --------------------------------------------------------

    df = df.withColumn(
        "is_delete_boundary",

        F.when(
            (F.col("op") == "DELETE")
            & F.col("previous_op").isNotNull()
            & (F.col("previous_op") != "DELETE"),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    return df


# ============================================================
# 8. BUILD SCD2 TIMELINE
# ============================================================

def build_scd2_timeline(df: DataFrame) -> DataFrame:
    """
    Build timeline markers from:
        1. New SCD2 versions
        2. DELETE events

    The next marker timestamp becomes END_AT.
    """

    # --------------------------------------------------------
    # New version records
    # --------------------------------------------------------

    version_starts = (
        df
        .filter(F.col("is_new_version"))
        .select(
            "restaurant_id",
            "name",
            "cuisine",
            "zone_id",
            "rating",
            "is_active",
            F.col("op_ts").alias("START_AT"),
            F.lit("VERSION").alias("marker_type")
        )
    )

    # --------------------------------------------------------
    # DELETE boundary records
    # --------------------------------------------------------

    delete_markers = (
        df
        .filter(F.col("is_delete_boundary"))
        .select(
            "restaurant_id",
            F.lit(None).cast("string").alias("name"),
            F.lit(None).cast("string").alias("cuisine"),
            F.lit(None).cast("string").alias("zone_id"),
            F.lit(None).cast("double").alias("rating"),
            F.lit(None).cast("boolean").alias("is_active"),
            F.col("op_ts").alias("START_AT"),
            F.lit("DELETE").alias("marker_type")
        )
    )

    # --------------------------------------------------------
    # Combine both marker types
    # --------------------------------------------------------

    timeline = version_starts.unionByName(delete_markers)

    timeline_window = (
        Window
        .partitionBy("restaurant_id")
        .orderBy("START_AT")
    )

    timeline = timeline.withColumn(
        "END_AT",
        F.lead("START_AT").over(timeline_window)
    )

    # Keep only actual version records.
    # DELETE markers only provide the closing timestamp.
    result = (
        timeline
        .filter(F.col("marker_type") == "VERSION")
        .select(
            "restaurant_id",
            "name",
            "cuisine",
            "zone_id",
            "rating",
            "is_active",
            "START_AT",
            "END_AT"
        )
    )

    return result


# ============================================================
# 9. APPLY TYPE 1 ATTRIBUTES
# ============================================================

def apply_type1_attributes(
    versions_df: DataFrame,
    valid_df: DataFrame
) -> DataFrame:
    """
    Apply latest name and cuisine values to all historical
    versions for each restaurant.

    Assumption:
        name and cuisine are Type 1 attributes.
    """

    latest_window = (
        Window
        .partitionBy("restaurant_id")
        .orderBy("op_ts")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.unboundedFollowing
        )
    )

    latest_attributes = (
        valid_df
        .withColumn(
            "latest_name",
            F.last("name", ignorenulls=True).over(latest_window)
        )
        .withColumn(
            "latest_cuisine",
            F.last("cuisine", ignorenulls=True).over(latest_window)
        )
        .select(
            "restaurant_id",
            "latest_name",
            "latest_cuisine"
        )
        .dropDuplicates(["restaurant_id"])
    )

    result = (
        versions_df
        .join(latest_attributes, "restaurant_id", "left")
        .withColumn(
            "name",
            F.coalesce("latest_name", "name")
        )
        .withColumn(
            "cuisine",
            F.coalesce("latest_cuisine", "cuisine")
        )
        .drop("latest_name", "latest_cuisine")
    )

    return result


# ============================================================
# 10. WRITE SILVER SCD2 TABLE
# ============================================================

def write_silver_table(scd2_df: DataFrame) -> None:
    """
    Write final SCD2 result to Silver.
    """

    final_df = scd2_df.select(
        F.col("restaurant_id").cast("string"),
        F.col("name").cast("string"),
        F.col("cuisine").cast("string"),
        F.col("zone_id").cast("string"),
        F.col("rating").cast("double"),
        F.col("is_active").cast("boolean"),
        F.col("START_AT").cast("timestamp"),
        F.col("END_AT").cast("timestamp")
    )

    (
        final_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE)
    )


# ============================================================
# 11. MAIN FUNCTION
# ============================================================

def main():

    print("Starting Restaurant SCD2 pipeline...")

    # --------------------------------------------------------
    # Step 1: Read Bronze
    # --------------------------------------------------------

    bronze_df = read_bronze_data()

    print("Bronze records:", bronze_df.count())

    # --------------------------------------------------------
    # Step 2: Standardize
    # --------------------------------------------------------

    standardized_df = standardize_data(bronze_df)

    # --------------------------------------------------------
    # Step 3: Read zone master
    # --------------------------------------------------------

    zones_df = read_zone_master()

    # --------------------------------------------------------
    # Step 4: Quarantine invalid records
    # --------------------------------------------------------

    valid_df, invalid_df = quarantine_invalid_records(
        standardized_df,
        zones_df
    )

    print("Valid records:", valid_df.count())
    print("Invalid records:", invalid_df.count())

    # --------------------------------------------------------
    # Step 5: Write quarantine
    # --------------------------------------------------------

    write_quarantine(invalid_df)

    print("Quarantine table updated:", QUARANTINE_TABLE)

    # --------------------------------------------------------
    # Step 6: Identify SCD2 changes
    # --------------------------------------------------------

    changes_df = identify_scd2_changes(valid_df)

    # --------------------------------------------------------
    # Step 7: Build SCD2 timeline
    # --------------------------------------------------------

    scd2_df = build_scd2_timeline(changes_df)

    # --------------------------------------------------------
    # Step 8: Apply Type 1 attributes
    # --------------------------------------------------------

    scd2_df = apply_type1_attributes(
        scd2_df,
        valid_df
    )

    # --------------------------------------------------------
    # Step 9: Write Silver
    # --------------------------------------------------------

    write_silver_table(scd2_df)

    print("Silver SCD2 table updated:", SILVER_TABLE)

    print("Final SCD2 records:", scd2_df.count())

    print("Restaurant SCD2 pipeline completed successfully.")


# ============================================================
# 12. EXECUTE
# ============================================================

main()