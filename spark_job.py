import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, to_timestamp, concat, round as spark_round
)

# -----------------------------------------------------------------------------
# Core processing function
# -----------------------------------------------------------------------------
def process_transactions(
    trans_df: DataFrame,
    cardholders_df: DataFrame,
    time_format: str = "HH:mm:ss"
) -> DataFrame:
    """
    Apply validations, transformations, enrichment, and formatting
    to a transactions DataFrame, returning the enriched DataFrame.
    """
    # 1) Validations
    df = trans_df.filter(
        (col("transaction_amount") > 0) &
        (col("transaction_status").isin("SUCCESS", "FAILED", "PENDING")) &
        col("cardholder_id").isNotNull() &
        col("merchant_id").isNotNull()
    )

    # 2) Category, timestamp, high_risk, merchant_info
    df = (
        df.withColumn(
            "transaction_category",
            when(col("transaction_amount") <= 100, lit("Low"))
            .when((col("transaction_amount") > 100) & (col("transaction_amount") <= 500), lit("Medium"))
            .otherwise(lit("High"))
        )
        .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp")))
        .withColumn(
            "high_risk",
            (col("fraud_flag") == True) |
            (col("transaction_amount") > 10000) |
            (col("transaction_category") == "High")
        )
        .withColumn(
            "merchant_info",
            concat(col("merchant_name"), lit(" - "), col("merchant_location"))
        )
    )

    # 3) Enrich with cardholders
    df = df.join(cardholders_df, on="cardholder_id", how="left")

    # 4) Update reward points
    df = df.withColumn(
        "updated_reward_points",
        col("reward_points") + spark_round(col("transaction_amount") / 10)
    )

    # 5) Fraud risk level
    df = df.withColumn(
        "fraud_risk_level",
        when(col("high_risk"), lit("Critical"))
        .when((col("risk_score") > 0.3) | (col("fraud_flag")), lit("High"))
        .otherwise(lit("Low"))
    )

    return df


# -----------------------------------------------------------------------------
# Standalone job entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Advanced Credit Card Transactions Processor") \
        .getOrCreate()

    # Paths and BigQuery tables
    json_file_path = sys.argv[1] if len(sys.argv) > 1 else 'gs://credit-card-data-analysis-gds/transactions/transactions_*.json'
    BQ_PROJECT_ID = "mythic-aloe-457912-d5"
    BQ_DATASET = "credit_card"
    BQ_CARDHOLDERS_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET}.cardholders_tb"
    BQ_TRANSACTIONS_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET}.transactions"

    # Load data
    cardholders_df = spark.read.format("bigquery") \
        .option("table", BQ_CARDHOLDERS_TABLE) \
        .load()
    transactions_df = spark.read.option("multiline","true").json(json_file_path)

    # Process
    enriched_df = process_transactions(transactions_df, cardholders_df)

    # Write to BigQuery
    enriched_df.write.format("bigquery") \
        .option("table", BQ_TRANSACTIONS_TABLE) \
        .option("temporaryGcsBucket", "bq-temp-gds") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .save()

    print("Advanced Transactions Processing Completed!")
    spark.stop()