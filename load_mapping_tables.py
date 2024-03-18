from google.cloud import bigquery
from structlog import get_logger
import pandas as pd


from retrieve_data import init_bq_conn

DATASET = "raw_data"
logger = get_logger()


def load_to_bq():
    (credentials, client) = init_bq_conn()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    tables = ["cloud_cover", "lifted_index", "rh2m", "wind10m"]
    for table in tables:
        logger.info("Uploading: ", table=table)
        data = pd.read_csv(f"./mapping_tables/{table}.csv")
        bq_table = f"{credentials.project_id}.{DATASET}.mapping_table_{table}"
        job = client.load_table_from_dataframe(data, bq_table, job_config=job_config)
        job.result()


if __name__ == "__main__":
    logger.info("Starting Run")
    load_to_bq()
    logger.info("Finished loading mapping tables")
