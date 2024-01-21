import json
import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

DATASET = "raw_data"


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
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    tables = ["cloud_cover", "lifted_index", "rh2m", "wind10m"]
    for table in tables:
        data = pd.read_csv(f"./mapping_tables/{table}.csv")
        bq_table = f"{credentials.project_id}.{DATASET}.mapping_table_{table}"
        job = client.load_table_from_dataframe(data, bq_table, job_config=job_config)
        job.result()


if __name__ == "__main__":
    load_to_bq()
