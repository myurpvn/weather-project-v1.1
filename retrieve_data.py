from requests import get
from datetime import timedelta
from datetime import datetime
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
import argparse

RETRY_COUNT = 3
FILE_DATE = datetime.now().date() + timedelta(days=1)
OUTPUT_PATH = "./output/"


def get_response(long, lat, mode, output):
    try:
        url = f"http://www.7timer.info/bin/api.pl?lon={long}&lat={lat}&product={mode}&output={output}"
        response = get(url).json()
        # print(response.text)
    except Exception as e:
        print("Error encountered: ", e)
        return None
    else:
        return response


def fix_date(row):
    return (row["date"] + timedelta(days=np.floor(row["timepoint"] / 24))).date()


def fix_timepoint(row):
    return row["timepoint"] % 24


def build_df(response, file_date) -> pd.DataFrame:
    df = pd.DataFrame()
    cols = []
    data = response["dataseries"]

    for i in data:
        row = pd.json_normalize(i, max_level=1)
        df = pd.concat([df, row])

    for col in df.columns:
        cols.append(col.replace(".", "_").lower())

    df.columns = cols
    df["date"] = response["init"][:8]
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    df["date"] = df.apply(fix_date, axis=1)
    df["timepoint"] = df.apply(fix_timepoint, axis=1)
    df = df[df["date"] == file_date]

    return df


def save_df(df: pd.DataFrame, file_date) -> pd.DataFrame:
    df.to_parquet(f"./output/{file_date}.pq", engine="pyarrow")
    return df


def init_bq_conn() -> (service_account.Credentials, bigquery.Client):
    json_acct_info = json.loads(os.getenv("BQ_JSON"))
    credentials = service_account.Credentials.from_service_account_info(
        json_acct_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    return (credentials, client)


def load_to_bq():
    (credentials, client) = init_bq_conn()
    table = f"{credentials.project_id}.raw_data.astro_weather"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    load_time = datetime.now()
    data = pd.read_parquet("./output/", engine="pyarrow")
    data["bq_load_time"] = load_time
    job = client.load_table_from_dataframe(data, table, job_config=job_config)
    job.result()

    table = client.get_table(table)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table
        )
    )


def initial_check():
    if not os.path.exists(OUTPUT_PATH):
        os.mkdir(OUTPUT_PATH)


if __name__ == "__main__":
    initial_check()
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", help="indicate local execution", action="store_true")
    args = parser.parse_args()

    response = None
    for _ in range(RETRY_COUNT):
        response = get_response("81.69", "7.71", "astro", "json")
        if response is not None:
            break

    if response is not None:
        df = build_df(response, FILE_DATE)
        save_df(df, FILE_DATE)

        if args.local:
            print("load job skipped for local run")
        else:
            load_to_bq()
