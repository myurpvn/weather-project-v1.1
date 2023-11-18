from requests import get
from datetime import timedelta
from datetime import datetime
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os


def get_response(long, lat, mode, output):
    url = f"http://www.7timer.info/bin/api.pl?lon={long}&lat={lat}&product={mode}&output={output}"
    response = get(url).json()
    # print(response.text)
    return response


def fix_date(row):
    return (row["date"] + timedelta(days=np.floor(row["timepoint"] / 24))).date()


def fix_timepoint(row):
    return row["timepoint"] % 24


def build_df(response, curr_date) -> pd.DataFrame:
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
    df = df[df["date"] == curr_date]
    return df


def save_df(df: pd.DataFrame, curr_date) -> pd.DataFrame:
    df.to_parquet(f"./output/{curr_date}.pq", engine="pyarrow")
    return df


def load_to_bq():
    credentials = service_account.Credentials.from_service_account_file(
        "cred.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    table = f"{credentials.project_id}.raw_data.astro_weather"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    load_time = datetime.now()
    data = pd.read_parquet("./output/", engine="pyarrow")
    data['bq_load_time'] = load_time
    job = client.load_table_from_dataframe(data, table, job_config=job_config)
    job.result()

    table = client.get_table(table)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table
        )
    )


def initial_checks():
    output_path = "./output/"
    is_exist = os.path.exists(output_path)
    if not is_exist:
        os.mkdir(output_path)


if __name__ == "__main__":
    initial_checks()
    curr_date = datetime.now().date()
    df = build_df(get_response("81.69", "7.71", "astro", "json"), curr_date)
    save_df(df, curr_date)
    load_to_bq()
