from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task()
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS."""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("taxi-gcs")
    gcs_block.get_directory(
        from_path=gcs_path,
        local_path='gcs_data/'
    )

    return Path(f"gcs_data/{gcs_path}")


@task()
def transform(path: Path, fillna: bool) -> pd.DataFrame:
    """Data cleaning example."""
    df = pd.read_parquet(path)
    if fillna:
        print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
        df['passenger_count'].fillna(0, inplace=True)
        print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame, color: str) -> None:
    """Write DataFrame to BigQuery."""
    gcp_creds = GcpCredentials.load("taxi-gcp-creds")

    df.to_gbq(
        destination_table=f'trips_data_all.{color}_non_partitioned',
        project_id='still-primer-402318',
        credentials=gcp_creds.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str):
    """Main ETL flow to load data into Big Query."""
    # color = "yellow"
    # year = 2021
    # month = 1
    
    path = extract_from_gcs(color, year, month)
    df = transform(path, False)
    write_bq(df, color)


@flow()
def etl_parent_flow_gtb_all(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    etl_parent_flow_gtb_all()
