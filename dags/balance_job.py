# -*- coding: utf-8 -*-
import decimal
import argparse
from typing import List
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import SchemaField
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import DecimalType, StringType


class BalanceSnapshot:
    def __init__(self, bucket, project_id, dataset_name, table_name, execution_date: str):
        self.bucket = bucket
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.table_name = table_name
        # in production, "snapshot_date + 1 days = execution_date" but for readability
        self.end_date = execution_date
        # genesis snapshot
        self.spark = SparkSession.builder.master("yarn").appName("daily-snapshot-token-transfer").getOrCreate()
        self.sc = self.spark.sparkContext

        erc_721 = [
            "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d",  # bored-ape-yacht-club
            "0x60e4d786628fea6478f785a6d7e704777c86a7c6",  # mutant-ape-yacth-club
            "0x8a90cab2b38dba80c64b7734e58ee1db38b8992e",  # Doodles
            "0x49cf6f5d44e70224e2e23fdcdd2c053f30ada28b",  # CloneX
            "0x1a92f7381b9f03921564a437210bb9396471050c",  # Cool Cats
        ]
        erc_20 = [
            "0xbb0e17ef65f82ab018d8edd776e8dd940327b28b",  # AXS
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",  # UNI
            "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0",  # MATIC
        ]
        all_target = erc_721 + erc_20

        self.erc_721 = self.sc.broadcast(erc_721)
        self.erc_20 = self.sc.broadcast(erc_20)
        self.target = self.sc.broadcast(all_target)

        self.spark.conf.set("temporaryGcsBucket", self.bucket)

    def run(self):
        transfers, prev_snapshot = self.load_data_from_bigquery()
        erc_20_transfers, erc_721_transfers = self.filter_transfers_data(transfers)
        divided_df_list: List[DataFrame] = self.aggregate_daily_data(erc_20_transfers, erc_721_transfers)
        joined_df = self.join_agg_df_on_prev_snapshot(prev_snapshot, divided_df_list)
        (
            joined_df
            # copy column since write.json removes partitioned key in every row
            .withColumn("contract_address", F.col("token_address"))
            .repartition("token_address")
            .write.partitionBy("token_address")
            .mode("overwrite")
            .format("json")
            .save(f"gs://{self.bucket}/date={self.end_date}")
        )

    def load_data_from_bigquery(self):
        transfers = (
            self.spark.read.format("bigquery")
            .option("table", "bigquery-public-data:crypto_ethereum.token_transfers")
            .load()
        )

        # table existance check
        try:
            client = bigquery.Client()
            client.get_table(f"{self.project_id}.{self.dataset_name}.{self.table_name}")
        except NotFound:
            self.create_table()

        prev_snapshot = (
            self.spark.read.format("bigquery")
            .option("table", f"{self.project_id}:{self.dataset_name}.{self.table_name}")
            .load()
            .withColumnRenamed("contract_address", "token_address")
        )

        return transfers, prev_snapshot

    def filter_transfers_data(self, transfers):
        if self.end_date == "2021-12-01":
            transfers = transfers.filter(F.to_date(F.col("block_timestamp"), "yyyy-MM-dd") <= self.end_date)
        else:
            transfers = transfers.filter(F.to_date(F.col("block_timestamp"), "yyyy-MM-dd") == self.end_date)

        filtered_transfers = transfers.filter(F.col("token_address").isin(self.target.value)).cache()

        erc_20_transfers = (
            filtered_transfers.filter(F.col("token_address").isin(self.erc_20.value))
            # wei overflow
            .withColumn("value", F.col("value").cast(DecimalType(precision=38, scale=0)))
        )

        erc_721_transfers = (
            filtered_transfers.filter(F.col("token_address").isin(self.erc_721.value))
            .withColumn("type", F.lit("ERC721"))
            .withColumnRenamed("value", "token_id")
        )

        return erc_20_transfers, erc_721_transfers

    @staticmethod
    def aggregate_daily_data(erc_20, erc_721):
        erc_20.cache()
        erc_20_add = (
            erc_20.groupby(["token_address", "to_address"])
            .agg(F.sum("value").alias("amount_wei_add"))
            .withColumnRenamed("to_address", "address")
            .withColumn("type", F.lit("ERC20"))
            .select("address", "type", "token_address", "amount_wei_add")
        )

        erc_20_sub = (
            erc_20.groupby(["token_address", "from_address"])
            .agg(F.sum("value").alias("amount_wei_sub"))
            .withColumnRenamed("from_address", "address")
            .withColumn("type", F.lit("ERC20"))
            .select("address", "type", "token_address", "amount_wei_sub")
        )

        erc_721.cache()
        erc_721_add = (
            erc_721.withColumn("amount_delta", F.lit(1))
            .withColumnRenamed("to_address", "address")
            .select("address", "type", "token_address", "token_id", "amount_delta")
        )

        erc_721_sub = (
            erc_721.withColumn("amount_delta", F.lit(-1))
            .withColumnRenamed("from_address", "address")
            .select("address", "type", "token_address", "token_id", "amount_delta")
        )

        return [erc_20_add, erc_20_sub, erc_721_add, erc_721_sub]

    def join_agg_df_on_prev_snapshot(self, prev_snapshot, divided_df_list):
        erc_20_add, erc_20_sub, erc_721_add, erc_721_sub = divided_df_list
        date_before = datetime.strftime(datetime.strptime(self.end_date, "%Y-%m-%d") - timedelta(days=1), "%Y-%m-%d")
        prev_snapshot = prev_snapshot.filter(F.to_date(F.col("date"), "yyyy-MM-dd") == date_before).cache()

        erc_20_joined_df = (
            prev_snapshot.filter(F.col("type") == "ERC20")
            .alias("prev")
            .withColumn("amount_delta", F.col("amount_delta").cast(DecimalType(precision=38, scale=0)))
            .join(erc_20_add.alias("add_20"), on=["address", "token_address"], how="outer")
            .select(
                "address",
                F.coalesce("prev.type", "add_20.type").alias("type"),
                "token_address",
                "token_id",
                "amount_delta", "amount_wei_add"
            ).alias("prev_add")
            .join(erc_20_sub.alias("sub_20"), on=["address", "token_address"], how="outer")
            .fillna(0, subset=["amount_delta", "amount_wei_add", "amount_wei_sub"])
            .withColumn("amount_delta", F.col("amount_delta") + F.col("amount_wei_add") - F.col("amount_wei_sub"))
            .select(
                "address",
                F.coalesce("prev_add.type", "sub_20.type").alias("type"),
                "token_address",
                "token_id",
                "amount_delta",
            )
        )

        erc_721_joined_df = (
            prev_snapshot.select("address", "type", "token_address", "token_id", "amount_delta")
            .filter(F.col("type") == "ERC721")
            .alias("prev")
            .union(erc_721_add)
            .union(erc_721_sub)
            .groupby(["address", "token_address", "token_id"])
            .agg(F.sum("amount_delta").alias("amount_delta"))
            .filter(~F.col("address").startswith("0x000000000000"))
            .withColumn("type", F.lit("ERC721"))
            .select("address", "type", "token_address", "token_id", "amount_delta")
        )

        @F.pandas_udf(StringType(), F.PandasUDFType.SCALAR)
        def to_wei(col: pd.Series) -> pd.Series:
            def decimal_to_str(number: decimal.Decimal) -> str:
                return str(int(number))

            return col.apply(decimal_to_str)

        joined_df = (
            erc_20_joined_df.union(erc_721_joined_df)
            .withColumn("amount_delta", to_wei("amount_delta"))
            .orderBy("address", "token_address")
            .withColumn("date", F.lit(self.end_date))
        )

        return joined_df

    def create_table(self):
        client = bigquery.Client()
        balance_schema = [
            SchemaField("address", "STRING", "REQUIRED"),
            SchemaField("type", "STRING", "REQUIRED"),
            SchemaField("contract_address", "STRING", "REQUIRED"),
            SchemaField("token_id", "INT64", "NULLABLE"),
            SchemaField("amount_delta", "STRING", "REQUIRED"),
            SchemaField("date", "DATE", "REQUIRED"),
        ]
        table = bigquery.Table(f"{self.project_id}.{self.dataset_name}.{self.table_name}", schema=balance_schema)
        assert client.create_table(table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="daily-balance-snapshot")
    parser.add_argument("--bucket", type=str)
    parser.add_argument("--project_id", type=str)
    parser.add_argument("--dataset_name", type=str)
    parser.add_argument("--table_name", type=str)
    parser.add_argument("--execution_date", type=str)
    args = parser.parse_args()
    job = BalanceSnapshot(args.bucket, args.project_id, args.dataset_name, args.table_name, args.execution_date)
    job.run()
