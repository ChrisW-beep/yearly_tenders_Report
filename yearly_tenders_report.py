import os
import csv
import boto3
import io
import pandas as pd
from datetime import datetime, timedelta

BUCKET_NAME = os.environ.get("SPIRITS_BUCKET", "spiritsbackups")  # ✅ Safe fallback
PREFIX_BASE = "processed_csvs/"
REPORT_DIR = "./reports/yearly"

s3 = boto3.client("s3")  # ✅ No profile needed!


def stream_csv_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()), dtype=str).fillna("")


def extract_ini_value(prefix, target):
    ini_key = f"{PREFIX_BASE}{prefix}/spirits.ini"
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=ini_key)
        content = obj["Body"].read().decode("utf-8")
        for line in content.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                if key.strip().lower() == target.lower():
                    return f'"{value.strip()}"'
    except s3.exceptions.NoSuchKey:
        return f'"Unknown{target}"'
    return f'"Unknown{target}"'


def process_prefix(prefix):
    try:
        jnl_key = f"{PREFIX_BASE}{prefix}/jnl.csv"
        str_key = f"{PREFIX_BASE}{prefix}/str.csv"

        df_jnl = stream_csv_from_s3(jnl_key)
        df_jnl["PRICE"] = pd.to_numeric(df_jnl["PRICE"], errors="coerce").fillna(0)

        store_name = "UnknownStore"
        try:
            df_str = stream_csv_from_s3(str_key)
            if "NAME" in df_str.columns and not df_str.empty:
                store_name = df_str.iloc[0]["NAME"]
        except Exception:
            pass

        df_jnl = df_jnl[df_jnl["RFLAG"] == "0"]

        # ✅ 12-month range
        today = datetime.today()
        first_day_this_month = today.replace(day=1)
        start_range = (first_day_this_month - pd.DateOffset(months=12)).date()
        end_range = (first_day_this_month - timedelta(days=1)).date()

        df_jnl["DATE_parsed"] = pd.to_datetime(df_jnl["DATE"], errors="coerce")
        df_jnl = df_jnl[
            (df_jnl["DATE_parsed"].dt.date >= start_range)
            & (df_jnl["DATE_parsed"].dt.date <= end_range)
        ]

        df_jnl["LINE_next"] = df_jnl["LINE"].shift(-1)
        df_jnl["DESCRIPT_next"] = df_jnl["DESCRIPT"].shift(-1)

        df_filtered = df_jnl[
            (df_jnl["LINE"] == "950") & (df_jnl["LINE_next"] == "980")
        ].copy()
        df_filtered["adj_PRICE"] = df_filtered["PRICE"]

        df_filtered["MONTH"] = df_filtered["DATE_parsed"].dt.to_period("M").astype(str)

        report = (
            df_filtered.groupby(["MONTH", "DESCRIPT_next"])
            .agg(sale_amount=("adj_PRICE", "sum"), sale_count=("adj_PRICE", "count"))
            .reset_index()
        )

        merchant_id = extract_ini_value(prefix, "DCMERCHANTID")
        ccprocessor = extract_ini_value(prefix, "DCPROCESSOR")
        cardinterface = extract_ini_value(prefix, "CardInterface")

        os.makedirs(REPORT_DIR, exist_ok=True)
        report_path = os.path.join(REPORT_DIR, f"yearly_{prefix}.csv")

        with open(report_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "Astoreid",
                    "Storename",
                    "MerchantID",
                    "CCProcessor",
                    "Month",
                    "Type",
                    "Sale_amount",
                    "Sale_count",
                    "CardInterface",
                    "currency",
                ]
            )
            for _, row in report.iterrows():
                writer.writerow(
                    [
                        prefix,
                        store_name,
                        merchant_id,
                        ccprocessor,
                        row["MONTH"],
                        row["DESCRIPT_next"],
                        row["sale_amount"],
                        row["sale_count"],
                        cardinterface,
                        "USD",
                    ]
                )
        print(f"✅ {prefix} → {report_path}", flush=True)

    except Exception as e:
        print(f"❌ Failed to process {prefix}: {e}", flush=True)


def main():
    paginator = s3.get_paginator("list_objects_v2")
    result = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX_BASE, Delimiter="/")

    prefixes = []
    for page in result:
        if "CommonPrefixes" in page:
            for p in page["CommonPrefixes"]:
                prefix = p["Prefix"].split("/")[-2]
                prefixes.append(prefix)

    for prefix in sorted(prefixes):
        process_prefix(prefix)


if __name__ == "__main__":
    main()
