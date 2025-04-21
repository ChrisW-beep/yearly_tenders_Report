# yearly_tenders_report.py
import os
import csv
import boto3
import io
import pandas as pd
from datetime import datetime, timedelta

BUCKET_NAME = "spiritsbackups"
PREFIX_BASE = "processed_csvs/"
REPORT_KEY = "yearly_reports/yearly_tenders_report.csv"

s3 = boto3.client("s3")

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
        return f'"N/A"'
    return f'"N/A"'

def process_prefix(prefix, rows):
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

        df_jnl["DATE_parsed"] = pd.to_datetime(df_jnl["DATE"], errors="coerce")

        # 👇 Inside the function, and correctly indented
        start_str = os.environ.get("START_DATE", "")
        end_str = os.environ.get("END_DATE", "")

        use_custom_dates = False
        try:
            if start_str and end_str:
                start_range = pd.to_datetime(start_str).date()
                end_range = pd.to_datetime(end_str).date()
                use_custom_dates = True
                print(f"📅 Using custom date range: {start_range} to {end_range}", flush=True)
        except Exception as e:
            print(f"⚠️ Invalid date format passed. Falling back to default 12-month range. Error: {e}", flush=True)

        if not use_custom_dates:
            today = datetime.today()
            first_day_this_month = today.replace(day=1)
            start_range = (first_day_this_month - pd.DateOffset(months=12)).date()
            end_range = (first_day_this_month - timedelta(days=1)).date()
            print(f"📅 Using default date range: {start_range} to {end_range}", flush=True)




        df_jnl = df_jnl[
            (df_jnl["DATE_parsed"].dt.date >= start_range) &
            (df_jnl["DATE_parsed"].dt.date <= end_range)
        ]

        df_jnl["LINE_next"] = df_jnl["LINE"].shift(-1)
        df_jnl["DESCRIPT_next"] = df_jnl["DESCRIPT"].shift(-1)

        df_filtered = df_jnl[
            (df_jnl["LINE"] == "950") & (df_jnl["LINE_next"] == "980") & (df_jnl["DESCRIPT_next"] != "")
        ].copy()

        df_filtered["MONTH"] = df_filtered["DATE_parsed"].dt.to_period("M").astype(str)

        # Identify returns based on PRICE < 0
        df_filtered["is_return"] = df_filtered["PRICE"] < 0

        df_filtered["sale_amount"] = df_filtered.apply(lambda row: row["PRICE"] if not row["is_return"] else 0, axis=1)
        df_filtered["sale_count"] = df_filtered.apply(lambda row: 1 if not row["is_return"] else 0, axis=1)
        df_filtered["reversal_amount"] = df_filtered.apply(lambda row: abs(row["PRICE"]) if row["is_return"] else 0, axis=1)
        df_filtered["reversal_count"] = df_filtered.apply(lambda row: 1 if row["is_return"] else 0, axis=1)

        report = (
            df_filtered.groupby(["MONTH", "DESCRIPT_next"])
            .agg(
                sale_amount=("sale_amount", "sum"),
                sale_count=("sale_count", "sum"),
                reversal_amount=("reversal_amount", "sum"),
                reversal_count=("reversal_count", "sum")
            )
            .reset_index()
        )

        merchant_id = extract_ini_value(prefix, "DCMERCHANTID")
        ccprocessor = extract_ini_value(prefix, "DCPROCESSOR")
        cardinterface = extract_ini_value(prefix, "CardInterface")

        for _, row in report.iterrows():
            rows.append([
                prefix,
                store_name,
                merchant_id,
                ccprocessor,
                row["MONTH"],
                row["DESCRIPT_next"],
                row["sale_amount"],
                row["sale_count"],
                row["reversal_amount"],
                row["reversal_count"],
                cardinterface,
                "USD",
            ])

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

    all_rows = []
    for prefix in sorted(prefixes):
        process_prefix(prefix, all_rows)

    # Write to single CSV
    output_csv = io.StringIO()
    writer = csv.writer(output_csv)
    writer.writerow([
        "Astoreid",
        "Storename",
        "MerchantID",
        "CCProcessor",
        "Month",
        "Type",
        "Sale_amount",
        "Sale_count",
        "Reversal_amount",
        "Reversal_count",
        "CardInterface",
        "currency",
    ])
    writer.writerows(all_rows)

    # Upload to S3
    s3.put_object(Bucket=BUCKET_NAME, Key=REPORT_KEY, Body=output_csv.getvalue().encode("utf-8"))
    print(f"✅ Uploaded single yearly report to s3://{BUCKET_NAME}/{REPORT_KEY}", flush=True)

if __name__ == "__main__":
    main()
