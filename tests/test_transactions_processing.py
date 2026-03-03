# Make sure these libraries are installed
# pip install pytest
# pip install pyspark

import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, IntegerType
)
from spark_job import process_transactions

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("PySparkUnitTest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

class TestTransactionProcessing:
    @pytest.fixture(autouse=True)
    def setup_data(self, spark):
        txn_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("cardholder_id", StringType(), False),
            StructField("merchant_id", StringType(), False),
            StructField("merchant_name", StringType(), False),
            StructField("merchant_category", StringType(), False),
            StructField("transaction_amount", DoubleType(), False),
            StructField("transaction_timestamp", StringType(), False),
            StructField("transaction_status", StringType(), False),
            StructField("fraud_flag", BooleanType(), False),
            StructField("merchant_location", StringType(), False),
        ])
        data = [
            ("T001","CH001","M001","Walmart","Groceries", 120.50, "2025-02-04T10:00:00Z","SUCCESS",False,"NY,USA"),
            ("T002","CH002","M002","Expedia","Travel", 9500.75, "2025-02-04T12:30:00Z","PENDING",True, "TO,CA"),
            ("T003","CH003","M003","Amazon","Shopping", 75.20,  "2025-02-04T15:45:00Z","FAILED", False,"SF,USA"),
        ]
        self.txn_df = spark.createDataFrame(data, schema=txn_schema)

        card_schema = StructType([
            StructField("cardholder_id", StringType(), False),
            StructField("customer_name", StringType(), False),
            StructField("reward_points", IntegerType(), False),
            StructField("risk_score", DoubleType(), False),
        ])
        cards = [
            ("CH001","John Doe",4500,0.15),
            ("CH002","Jane Smith",1200,0.35),
            ("CH003","Ali Khan",8000,0.10),
        ]
        self.card_df = spark.createDataFrame(cards, schema=card_schema)

    def test_categories(self):
        df = process_transactions(self.txn_df, self.card_df)
        mapping = {r['transaction_id']: r['transaction_category'] for r in df.select('transaction_id','transaction_category').collect()}
        assert mapping['T001']=='Medium'
        assert mapping['T002']=='High'
        assert mapping['T003']=='Low'

    def test_risk_and_points(self):
        df = process_transactions(self.txn_df, self.card_df)
        rows = {r['transaction_id']: r.asDict() for r in df.collect()}
        assert rows['T002']['high_risk'] is True
        assert rows['T002']['fraud_risk_level']=='Critical'
        # assert rows['T002']['fraud_risk_level']=='High'
        assert rows['T001']['updated_reward_points']==4512
        assert rows['T003']['updated_reward_points']==8008

    def test_processing_verbose(self):
        df = process_transactions(self.txn_df, self.card_df)
        count = df.count()
        print(f"Processed {count} rows")
        assert count==3