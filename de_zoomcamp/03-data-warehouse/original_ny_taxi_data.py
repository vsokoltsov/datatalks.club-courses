import requests
from google.cloud import storage
from tqdm import tqdm
from threading import RLock
from concurrent.futures import ThreadPoolExecutor, as_completed

tqdm.set_lock(RLock())

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
BUCKET_NAME = "vs-data-engineering-zoomcamp-bucket"

def stream_to_gcs(client: storage.Client, url: str, gcs_path: str, position:int) -> None:
    chunk_size=1024 * 1024
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.chunk_size = 8 * chunk_size

    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()

        total = r.headers.get("Content-Length")
        total = int(total) if total and total.isdigit() else None

        r.raw.decode_content = True

        with tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            position=position,
            leave=False,
            desc=f"{gcs_path}",
        ) as pbar:
            with blob.open("wb", content_type=r.headers.get("Content-Type")) as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    pbar.update(len(chunk))

def run_parallel(jobs, max_workers=4):
    client = storage.Client()

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = []
        for i, (url, gcs_path) in enumerate(jobs):
            futures.append(ex.submit(stream_to_gcs, client, url, gcs_path, i))

        for fut in as_completed(futures):
            fut.result()

if __name__ == '__main__':
    jobs = []
    client = storage.Client()
    for ds_type in ['yellow', 'green']:
        for year in [2019, 2020]:
            for month in range(1, 13):
                mm = f"{month:02d}"
                file_name = f"{ds_type}_tripdata_{year}-{mm}.parquet"
                url = f"{BASE_URL}/trip-data/{file_name}"
                gcs_path = f"{ds_type}/{file_name}"
                jobs.append((url, gcs_path))
    run_parallel(jobs)

