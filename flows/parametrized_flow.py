from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket


DATE_COLUMS = {
    "yellow": ["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    "green": ["lpep_pickup_datetime", "lpep_dropoff_datetime"],
}


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame."""

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    date_columns = DATE_COLUMS[color]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col])
    # df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    # df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file."""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    path.parent.mkdir(exist_ok=True, parents=True)
    df.to_parquet(path, compression='gzip')
    return path


@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS."""
    gcs_block = GcsBucket.load("taxi-gcs")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main etl function."""
    # color = "yellow"
    # year = 2021
    # month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    etl_parent_flow()
