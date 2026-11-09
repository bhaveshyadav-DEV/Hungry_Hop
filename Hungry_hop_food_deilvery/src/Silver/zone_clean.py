from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# ============================================================
# 1. CONFIGURATION
# ============================================================

CATALOG = dbutils.widgets.text("CATALOG", "hop")
CATALOG = dbutils.widgets.get("CATALOG")

BRONZE_TABLE = f"{CATALOG}.bronze.zones"
SILVER_TABLE = f"{CATALOG}.silver.zones"
QUARANTINE_TABLE = f"{CATALOG}.silver.quarantine_zones"

# Allowed zone_type values (everything else is quarantined)
VALID_ZONE_TYPES = ["core", "suburb", "outskirts"]

def standardize_zones(df: DataFrame) -> DataFrame:
    """
    Standardize zone data and apply correct data types.

    1. Rename generic CSV columns (_c0/_c1/_c2) to business names.
    2. Drop the header row that was ingested as data.
    3. Preserve raw record.
    4. Cast columns into expected Silver data types.
    5. Remove unnecessary metadata columns.
    """

    # --------------------------------------------------------
    # Step 1: Rename generic columns to business names
    # (Bronze CSV was ingested without header, so the first
    # data row is actually the header)
    # --------------------------------------------------------

    if "_c0" in df.columns:
        df = (
            df
            .withColumnRenamed("_c0", "zone_id")
            .withColumnRenamed("_c1", "city")
            .withColumnRenamed("_c2", "zone_type")
        )

    # --------------------------------------------------------
    # Step 2: Drop the ingested header row
    # --------------------------------------------------------

    df = df.filter(F.upper(F.trim(F.col("zone_id"))) != "ZONE_ID")

    # --------------------------------------------------------
    # Step 3: Preserve original Bronze record
    # --------------------------------------------------------

    business_columns = [c for c in ["zone_id", "city", "zone_type"] if c in df.columns]

    df = df.withColumn(
        "raw_record",
        F.to_json(
            F.struct(
                *[F.col(c) for c in business_columns]
            )
        )
    )

    # --------------------------------------------------------
    # Step 4: Correct data type casting + standardization
    # --------------------------------------------------------

    df = (
        df
        .withColumn("zone_id", F.trim(F.col("zone_id").cast("STRING")))
        .withColumn("city", F.trim(F.col("city").cast("STRING")))
        .withColumn("zone_type", F.lower(F.trim(F.col("zone_type").cast("STRING"))))
    )

    # --------------------------------------------------------
    # Step 5: Remove unnecessary metadata columns
    # --------------------------------------------------------

    metadata_columns = [
        "_rescued_data",
        "_ingested_at",
        "_source_file"
    ]

    existing_metadata = [
        c for c in metadata_columns if c in df.columns
    ]

    df = df.drop(*existing_metadata)

    return df

# ============================================================
# 3. VALIDATE ZONE DATA
# ============================================================

def validate_zones(df: DataFrame):
    """
    Quarantine zones whose zone_id is NULL or blank.
    Also quarantine invalid zone_type or missing city.
    """

    invalid_id_condition = (
        F.col("zone_id").isNull()
        | (F.trim(F.col("zone_id")) == "")
        | (F.lower(F.trim(F.col("zone_id"))) == "null")
    )

    invalid_type_condition = (
        F.col("zone_type").isNull()
        | (F.trim(F.col("zone_type")) == "")
        | (~F.col("zone_type").isin(VALID_ZONE_TYPES))
    )

    invalid_city_condition = (
        F.col("city").isNull()
        | (F.trim(F.col("city")) == "")
        | (F.lower(F.trim(F.col("city"))) == "null")
    )

    any_invalid = (
        invalid_id_condition
        | invalid_type_condition
        | invalid_city_condition
    )

    # Invalid records with a specific reject reason
    invalid_df = (
        df
        .filter(any_invalid)
        .withColumn(
            "reject_reason",
            F.when(invalid_id_condition, F.lit("ZONE_ID_IS_NULL_OR_BLANK"))
            .when(invalid_type_condition, F.lit("ZONE_TYPE_INVALID"))
            .otherwise(F.lit("CITY_IS_NULL_OR_BLANK"))
        )
        .withColumn(
            "quarantined_at",
            F.current_timestamp()
        )
    )

    # Valid records
    valid_df = df.filter(~any_invalid)

    return valid_df, invalid_df

# ============================================================
# 4. PREPARE SILVER OUTPUT
# ============================================================

def prepare_silver_output(valid_df: DataFrame) -> DataFrame:
    """
    Select only required business columns for Silver.
    """

    silver_df = valid_df.select(
        "zone_id",
        "city",
        "zone_type"
    )

    return silver_df

# ============================================================
# 5. PREPARE QUARANTINE OUTPUT
# ============================================================

def prepare_quarantine_output(invalid_df: DataFrame) -> DataFrame:
    """
    Keep rejected zone data with rejection reason.
    Metadata columns are not included.
    """

    quarantine_df = invalid_df.select(
        "zone_id",
        "city",
        "zone_type",
        "raw_record",
        "reject_reason",
        "quarantined_at"
    )

    return quarantine_df

    # ============================================================
# 6. WRITE SILVER TABLE
# ============================================================

def write_silver_table(df: DataFrame):
    """
    Write valid zones into Silver using Spark overwrite.
    No MERGE INTO is used.
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE)
    )


# ============================================================
# 7. WRITE QUARANTINE TABLE
# ============================================================

def write_quarantine_table(df: DataFrame):
    """
    Write invalid zones into quarantine table.
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(QUARANTINE_TABLE)
    )


# ============================================================
# 8. MAIN FUNCTION
# ============================================================

def main():

    # Step 1: Read Bronze zones
    bronze_df = spark.table(BRONZE_TABLE)

    print("Bronze zone count:", bronze_df.count())

    # Step 2: Standardize zone data
    standardized_df = standardize_zones(bronze_df)

    # Step 3: Validate zone data
    valid_df, invalid_df = validate_zones(standardized_df)

    # Step 4: Prepare outputs
    silver_df = prepare_silver_output(valid_df)
    quarantine_df = prepare_quarantine_output(invalid_df)

    # Step 5: Display counts
    print("Valid zone count:", silver_df.count())
    print("Invalid zone count:", quarantine_df.count())

    # Step 6: Write Silver table
    write_silver_table(silver_df)

    # Step 7: Write Quarantine table
    write_quarantine_table(quarantine_df)

    print("Zone Silver processing completed successfully.")

# ============================================================
# 9. EXECUTION
# ============================================================

if __name__ == "__main__":
    main()