import io
import os
import requests
from pathlib import Path
import pandas as pd
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "vs-de-zoomcamp-2026-hw4")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    # client.create_bucket(bucket)
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        DTYPES = {
            # идентификаторы/категории
            "store_and_fwd_flag": "string",

            # integers that may have nulls -> pandas nullable Int64
            "VendorID": "Int64",
            "RatecodeID": "Int64",
            "PULocationID": "Int64",
            'PUlocationID': "Int64",
            "DOLocationID": "Int64",
            "DOlocationID": "Int64",
            "payment_type": "Int64",
            "passenger_count": "Int64",
            "trip_type": "Int64",   # есть в green

            # float-like fees that may be empty
            "ehail_fee": "Float64",  # green
            "airport_fee": "Float64", # yellow (если используешь csv с airport_fee)
        }
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        # download it using requests via a pandas df
        if not Path(file_name).is_file():
            request_url = f"{init_url}{service}/{file_name}"
            r = requests.get(request_url)
            open(file_name, 'wb').write(r.content)
            print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(file_name, compression='gzip', low_memory=False)
        df_head = pd.read_csv(file_name, compression="gzip", nrows=0)
        print(list(df_head.columns))
        print("col #3 =", df_head.columns[3])

        file_name = file_name.replace('.csv.gz', '.parquet')
        for col, dtype in DTYPES.items():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
        df.to_parquet(file_name, engine='pyarrow', index=False)
        print(f"Parquet: {file_name}")

        # # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


if __name__ == '__main__':
    web_to_gcs('2019', 'fhv')
    # web_to_gcs('2020', 'green')
    # web_to_gcs('2019', 'yellow')
    # web_to_gcs('2020', 'yellow')


