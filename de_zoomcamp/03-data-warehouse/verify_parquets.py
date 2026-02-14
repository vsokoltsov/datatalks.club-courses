from collections import defaultdict
from google.cloud import storage
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from tqdm import tqdm

BUCKET = "vs-data-engineering-zoomcamp-bucket"

def list_gcs_parquets(client: storage.Client, prefix: str):
    bucket = client.bucket(BUCKET)
    for blob in client.list_blobs(bucket, prefix=prefix):
        if blob.name.endswith(".parquet"):
            yield blob.name

def parquet_schema_types(gcs_path: str):
    # gcs_path вида: "yellow/yellow_tripdata_2020-07.parquet"
    fs = pafs.GcsFileSystem()  # использует те же креды, что и google-auth
    with fs.open_input_file(f"{BUCKET}/{gcs_path}") as f:
        pf = pq.ParquetFile(f)
        schema = pf.schema_arrow  # Arrow schema
    return {field.name: str(field.type) for field in schema}

def verify_prefix(prefix: str):
    client = storage.Client()
    files = list(list_gcs_parquets(client, prefix))
    col_types = defaultdict(set)
    missing_cols = defaultdict(int)

    for name in tqdm(files, desc=f"Scanning {prefix}"):
        types = parquet_schema_types(name)
        # собираем типы по колонкам
        for col, t in types.items():
            col_types[col].add(t)

        # считаем "отсутствия" колонок (полезно, если в одних файлах колонки нет)
        for col in col_types.keys():
            if col not in types:
                missing_cols[col] += 1

    # выводим расхождения типов
    mismatches = {c: sorted(list(ts)) for c, ts in col_types.items() if len(ts) > 1}
    return {
        "num_files": len(files),
        "mismatched_types": mismatches,
        "missing_counts": dict(missing_cols),
    }

if __name__ == "__main__":
    for p in ["yellow/", "green/"]:
        report = verify_prefix(p)
        print("\n===", p, "===")
        print("files:", report["num_files"])
        print("mismatched columns:", report["mismatched_types"])