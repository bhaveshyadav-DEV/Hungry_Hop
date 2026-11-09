from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# ============================================================
# 1. CONFIGURATION
# ============================================================

CATALOG = dbutils.widgets.text("CATALOG", "hop")
CATALOG = dbutils.widgets.get("CATALOG")

BRONZE_TABLE = f"{CATALOG}.bronze.customers"
SILVER_TABLE = f"{CATALOG}.silver.customers"
QUARANTINE_TABLE = f"{CATALOG}.silver.quarantine_customers"


def standardize_customers(df: DataFrame) -> DataFrame:
    """
    Standardize customer data and apply correct data types.

    1. Preserve raw record.
    2. Cast columns into expected Silver data types.
    3. Standardize email and phone.
    4. Parse signup_date into DATE.
    5. Remove unnecessary metadata columns.
    """

    # --------------------------------------------------------
    # Step 1: Preserve original Bronze record
    # --------------------------------------------------------

    df = df.withColumn(
        "raw_record",
        F.to_json(
            F.struct(
                *[F.col(c) for c in df.columns]
            )
        )
    )

    # --------------------------------------------------------
    # Step 2: Correct data type casting
    # --------------------------------------------------------

    df = (
        df
        .withColumn("customer_id", F.col("customer_id").cast("STRING"))
        .withColumn("name", F.col("name").cast("STRING"))
        .withColumn("email", F.col("email").cast("STRING"))
        .withColumn("phone", F.col("phone").cast("STRING"))
        .withColumn("city", F.col("city").cast("STRING"))
        .withColumn("tier", F.col("tier").cast("STRING"))

        # Safe conversion to DATE
        .withColumn(
            "signup_date",
            F.expr("try_cast(signup_date AS DATE)")
        )
    )

    # --------------------------------------------------------
    # Step 3: String standardization
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "customer_id",
            F.trim(F.col("customer_id"))
        )
        .withColumn(
            "name",
            F.trim(F.col("name"))
        )
        .withColumn(
            "email",
            F.lower(F.trim(F.col("email")))
        )
        .withColumn(
            "city",
            F.trim(F.col("city"))
        )
        .withColumn(
            "tier",
            F.upper(F.trim(F.col("tier")))
        )
    )

    # --------------------------------------------------------
    # Step 4: Phone standardization
    # --------------------------------------------------------

    df = df.withColumn(
        "phone",
        F.regexp_replace(
            F.trim(F.col("phone")),
            r"^\+91\s*",
            ""
        )
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
# 3. VALIDATE CUSTOMER DATA
# ============================================================

def validate_customers(df: DataFrame):
    """
    Quarantine customers whose customer_id is NULL or blank.
    No reference-integrity validation is required.
    """

    invalid_condition = (
        F.col("customer_id").isNull()
        | (F.trim(F.col("customer_id")) == "")
        | (F.lower(F.trim(F.col("customer_id"))) == "null")
    )

    # Invalid records
    invalid_df = (
        df
        .filter(invalid_condition)
        .withColumn(
            "reject_reason",
            F.lit("CUSTOMER_ID_IS_NULL_OR_BLANK")
        )
        .withColumn(
            "quarantined_at",
            F.current_timestamp()
        )
    )

    # Valid records
    valid_df = df.filter(~invalid_condition)

    return valid_df, invalid_df


# ============================================================
# 4. PREPARE SILVER OUTPUT
# ============================================================

def prepare_silver_output(valid_df: DataFrame) -> DataFrame:
    """
    Select only required business columns for Silver.
    """

    silver_df = valid_df.select(
        "customer_id",
        "name",
        "email",
        "phone",
        "city",
        "tier",
        "signup_date"
    )

    return silver_df


# ============================================================
# 5. PREPARE QUARANTINE OUTPUT
# ============================================================

def prepare_quarantine_output(invalid_df: DataFrame) -> DataFrame:
    """
    Keep rejected customer data with rejection reason.
    Metadata columns are not included.
    """

    quarantine_df = invalid_df.select(
        "customer_id",
        "name",
        "email",
        "phone",
        "city",
        "tier",
        "signup_date",
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
    Write valid customers into Silver using Spark overwrite.
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
    Write invalid customers into quarantine table.
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

    # Step 1: Read Bronze customers
    bronze_df = spark.table(BRONZE_TABLE)

    print("Bronze customer count:", bronze_df.count())

    # Step 2: Standardize customer data
    standardized_df = standardize_customers(bronze_df)

    # Step 3: Validate customer data
    valid_df, invalid_df = validate_customers(standardized_df)

    # Step 4: Prepare outputs
    silver_df = prepare_silver_output(valid_df)
    quarantine_df = prepare_quarantine_output(invalid_df)

    # Step 5: Display counts
    print("Valid customer count:", silver_df.count())
    print("Invalid customer count:", quarantine_df.count())

    # Step 6: Write Silver table
    write_silver_table(silver_df)

    # Step 7: Write Quarantine table
    write_quarantine_table(quarantine_df)

    print("Customer Silver processing completed successfully.")

# ============================================================
# 9. EXECUTION
# ============================================================

if __name__ == "__main__":
    main()