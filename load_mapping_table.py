import json
import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

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
    table = f"{credentials.project_id}.raw_data.mapping_table"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    data = pd.read_csv("./mapping_table.csv")
    job = client.load_table_from_dataframe(data, table, job_config=job_config)
    job.result()

    table = client.get_table(table)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table
        )
    )